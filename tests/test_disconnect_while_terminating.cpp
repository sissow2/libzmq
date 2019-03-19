#include "testutil.hpp"
#include "testutil_unity.hpp"

#include <unity.h>

#define BIND_TOK "BIND\0"
#define UNBIND_TOK "UNBIND\0"

typedef unsigned long duration_t; // Microseconds
duration_t milliseconds (unsigned long ms) {
    return ms * 1000;
}

// Configuration shared between the two threads
typedef struct shared_config {
    duration_t disconnect_period; // A full disconnect-reconnect cycle happens in this period
    duration_t reconnect_duty_cycle; // Every time a disconnect period starts, T + this is when the reconnect happens
    duration_t test_duration; // Test will only run for this long

    int n_parts; // Number of parts to send in a multipart message
    const char* send_receive_endpoint; // endpoint that the test data will be going over
    const char* control_endpoint; // endpoint that the sender will send "please disconnect" signals over
} shared_config_t;

void sender_threadfn (void *opaque_config) 
{
    const shared_config_t *config = (shared_config_t *)opaque_config;
    void *ctx = zmq_ctx_new ();
    void *pubsock = zmq_socket (ctx, ZMQ_PUB);
    void *ctlsock = zmq_socket (ctx, ZMQ_PUSH);

    zmq_msg_t snd_msg;
    zmq_msg_t ctl_msg;

    void *stopwatch = zmq_stopwatch_start ();

    duration_t t_disconnect = zmq_stopwatch_intermediate (stopwatch);

    // Tracks if pubsock is currently connected
    bool is_connected = false;

    // Bind the control socket so we can tell the receiver when to connect/disconnect
    TEST_ASSERT_SUCCESS_ERRNO (zmq_bind (ctlsock, config->control_endpoint));

    int rc;

    while (true) {
        duration_t t_now = zmq_stopwatch_intermediate (stopwatch);
         
        if (t_now > config->test_duration) {
            // Go to cleanup below
            break;
        }

        // Simulate user disconnections and reconnections
        {
            if (is_connected) {
                if ((t_now - t_disconnect) > config->disconnect_period) {
                    TEST_ASSERT_SUCCESS_ERRNO (zmq_unbind (pubsock, config->send_receive_endpoint));

                    zmq_send (ctlsock, UNBIND_TOK, strlen(UNBIND_TOK) + 1, ZMQ_DONTWAIT);
                    t_disconnect = t_now;
                    is_connected = false;
                }
            } else {
                if ((t_now - t_disconnect) > config->reconnect_duty_cycle) {
                    TEST_ASSERT_SUCCESS_ERRNO (zmq_bind(pubsock, config->send_receive_endpoint));
                    assert(rc == 0);

                    zmq_send (ctlsock, BIND_TOK, strlen(BIND_TOK) + 1, ZMQ_DONTWAIT);
                    is_connected = true;
                }
            }
        }

        // If connected, send a message with "parts" parts
        if (is_connected) {
            int flags = ZMQ_SNDMORE;
            for(int i = 0; i < config->n_parts; ++i) {
                if (i == (config->n_parts - 1)) {
                    flags = 0;
                }

                zmq_msg_init_size (&snd_msg, 1024);
                TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_send (&snd_msg, pubsock, flags));
                zmq_msg_close (&snd_msg);
            }
        }
    }

    printf("shutting down publisher\n");

    zmq_stopwatch_stop (stopwatch);
    close_zero_linger (pubsock);
    close_zero_linger (ctlsock);
    zmq_ctx_term (ctx);

    printf ("publisher has shut down\n");
}

void receiver_threadfn (void* opaque_config) 
{
    const shared_config_t *config = (shared_config_t *)opaque_config;
    void *ctx = zmq_ctx_new ();
    void *subsock = zmq_socket (ctx, ZMQ_SUB);
    void *ctlsock = zmq_socket (ctx, ZMQ_PULL);

    zmq_msg_t rcv_msg;
    zmq_msg_t ctl_msg;

    void *stopwatch = zmq_stopwatch_start ();

    int n_full_msg = 0;
    bool is_connected = false;
    bool publisher_is_connected = false;
    int rc;

    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (subsock, ZMQ_SUBSCRIBE, "", 0));

    zmq_msg_init (&rcv_msg);

    // Connect the control socket so we know when to connect/disconnect
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (ctlsock, config->control_endpoint));

    while (true) {
        duration_t t_now = zmq_stopwatch_intermediate (stopwatch);

        if (t_now > config->test_duration) {
            // Go to cleanup below
            break;
        }

        {
            // Try to read from the ctl socket.  The publisher tells us if it has bound or unbound.
            zmq_msg_init (&ctl_msg);
            if (zmq_msg_recv (&ctl_msg, ctlsock, ZMQ_DONTWAIT) > 0) {
                if (strcmp ((const char *)zmq_msg_data (&ctl_msg), BIND_TOK) == 0) {
                    publisher_is_connected = true;
                }else if (strcmp ((const char *)zmq_msg_data (&ctl_msg), UNBIND_TOK) == 0) {
                    publisher_is_connected = false;
                }
            }else{
                TEST_ASSERT_EQUAL(errno, EAGAIN);
            }
        }

        {
            // Track the connection state of the sender.
            if (publisher_is_connected && !is_connected) {
                TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (subsock, config->send_receive_endpoint));
                is_connected = true;
            }

            if (!publisher_is_connected && is_connected) {
                TEST_ASSERT_SUCCESS_ERRNO (zmq_disconnect (subsock, config->send_receive_endpoint));
                is_connected = false;
            }
        }

        // Read all received multipart messages until EAGAIN is encountered (indicating there is
        // nothing else to read)
        bool continue_reading = true;
        while(continue_reading) {
            int n_part = 0;
            do {
                zmq_msg_close (&rcv_msg);
                zmq_msg_init (&rcv_msg);
                int flags = ZMQ_DONTWAIT;

                if (zmq_msg_recv (&rcv_msg, subsock, flags) == -1) {
                    TEST_ASSERT_EQUAL (errno, EAGAIN);
                    continue_reading = false;
                    break;
                }

                ++n_part;
            } while (zmq_msg_more (&rcv_msg));

            if (n_part > 0) {
                ++n_full_msg;
                TEST_ASSERT_EQUAL_MESSAGE(n_part, config->n_parts, "Received wrong number of messages");

                if (n_full_msg % 1000 == 0) {
                    printf("recv %d msgs\n", n_full_msg);
                }
            }
        }
    }

    printf ("shutting down subscriber\n");

    zmq_stopwatch_stop (stopwatch);
    close_zero_linger (subsock);
    close_zero_linger (ctlsock);
    zmq_ctx_term (ctx);

    printf ("subscriber has shut down\n");
}

void setUp ()
{
    setup_test_context ();
}

void tearDown ()
{
    teardown_test_context ();
}

void test_disconnect_while_terminating ()
{
    shared_config_t shared_config;

    shared_config.disconnect_period = milliseconds (100);
    shared_config.reconnect_duty_cycle = shared_config.disconnect_period / 2;
    shared_config.test_duration = milliseconds (5000);
    shared_config.n_parts = 100;
    shared_config.send_receive_endpoint = ENDPOINT_0;
    shared_config.control_endpoint = ENDPOINT_1;

    void* send_thread_handle = zmq_threadstart (&sender_threadfn, &shared_config);
    void* recv_thread_handle = zmq_threadstart (&receiver_threadfn, &shared_config);

    zmq_threadclose (send_thread_handle);
    zmq_threadclose (recv_thread_handle);
}

int main() {
    setup_test_environment ();

    UNITY_BEGIN ();
    RUN_TEST(test_disconnect_while_terminating);
    return UNITY_END ();
}
