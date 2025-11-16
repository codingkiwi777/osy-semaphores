//***************************************************************************
//
// Pizza Server
//
//***************************************************************************

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>

//***************************************************************************
// constants

#define N 10                        // number of slots in the pizza queue
#define MAX_PIZZA_NAME 64           // max pizza name length

#define SEM_MUTEX_NAME      "/sem_pizza_mutex"
#define SEM_EMPTY_NAME      "/sem_pizza_empty"
#define SEM_FULL_NAME       "/sem_pizza_full"

//***************************************************************************
// log messages

#define LOG_ERROR               0       // errors
#define LOG_INFO                1       // information and notifications
#define LOG_DEBUG               2       // debug messages

// debug flag
int g_debug = LOG_INFO;

void log_msg(int t_log_level, const char *t_form, ...)
{
    const char *out_fmt[] = 
    {
        "ERR: (%d-%s) %s\n",
        "INF: %s\n",
        "DEB: %s\n"
    };

    if ( t_log_level && t_log_level > g_debug ) return;

    char l_buf[ 1024 ];
    va_list l_arg;
    va_start( l_arg, t_form );
    vsprintf( l_buf, t_form, l_arg );
    va_end( l_arg );

    switch ( t_log_level )
    {
    case LOG_INFO:
    case LOG_DEBUG:
        fprintf( stdout, out_fmt[ t_log_level ], l_buf );
        break;

    case LOG_ERROR:
        fprintf( stderr, out_fmt[ t_log_level ], errno, strerror( errno ), l_buf );
        break;
    }
}

//***************************************************************************
// shared data structure - pizza queue

struct pizza_queue 
{
    char buffer[N][MAX_PIZZA_NAME];  // the buffer for pizza names
    int in;                          // index for producer (pekar)
    int out;                         // index for consumer (zakaznik)
};

//***************************************************************************
// global variables

sem_t *g_sem_mutex = nullptr;    // controls access to critical region
sem_t *g_sem_empty = nullptr;    // counts empty buffer slots
sem_t *g_sem_full = nullptr;     // counts full buffer slots

struct pizza_queue g_queue;      // shared pizza queue

//***************************************************************************
// producer function - inserts pizza into queue

void producer(char *pizza)
{
    /* down(&empty) */
    if ( sem_wait( g_sem_empty ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_wait(empty) failed" );
        return;
    }
    
    /* down(&mutex) */
    if ( sem_wait( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_wait(mutex) failed" );
        return;
    }
    
    /* insert pizza into buffer */
    strncpy( g_queue.buffer[ g_queue.in ], pizza, MAX_PIZZA_NAME - 1 );
    g_queue.buffer[ g_queue.in ][ MAX_PIZZA_NAME - 1 ] = '\0';
    log_msg( LOG_DEBUG, "Inserted pizza '%s' into queue at position %d", pizza, g_queue.in );
    g_queue.in = ( g_queue.in + 1 ) % N;
    
    /* up(&mutex) */
    if ( sem_post( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(mutex) failed" );
        return;
    }
    
    /* up(&full) */
    if ( sem_post( g_sem_full ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(full) failed" );
        return;
    }
}

//***************************************************************************
// consumer function - removes pizza from queue

void consumer(char *pizza_out)
{
    /* down(&full) */
    if ( sem_wait( g_sem_full ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_wait(full) failed" );
        return;
    }
    
    /* down(&mutex) */
    if ( sem_wait( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_wait(mutex) failed" );
        return;
    }
    
    /* remove pizza from buffer */
    strncpy( pizza_out, g_queue.buffer[ g_queue.out ], MAX_PIZZA_NAME - 1 );
    pizza_out[ MAX_PIZZA_NAME - 1 ] = '\0';
    log_msg( LOG_DEBUG, "Removed pizza '%s' from queue at position %d", pizza_out, g_queue.out );
    g_queue.out = ( g_queue.out + 1 ) % N;
    
    /* up(&mutex) */
    if ( sem_post( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(mutex) failed" );
        return;
    }
    
    /* up(&empty) */
    if ( sem_post( g_sem_empty ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(empty) failed" );
        return;
    }
}

//***************************************************************************
// cleaning function

void clean(void)
{
    log_msg(LOG_INFO, "Final cleaning ...");

    // clean semaphores
    if (g_sem_mutex)
    {
        sem_close(g_sem_mutex);
        sem_unlink(SEM_MUTEX_NAME);
    }
    
    if (g_sem_empty)
    {
        sem_close(g_sem_empty);
        sem_unlink(SEM_EMPTY_NAME);
    }
    
    if (g_sem_full)
    {
        sem_close(g_sem_full);
        sem_unlink(SEM_FULL_NAME);
    }
}

// catch signal = for example CTRL+C sig (signal)
void catch_sig(int t_sig)
{
  exit(1);
}

//***************************************************************************
// client thread function

void *client_thread( void *arg )
{
    int sock_client = *((int*)arg);
    delete (int*)arg;
    
    char buf[128];
    
    // send role question
    const char* role_question = "Role?\n";
    write( sock_client, role_question, strlen(role_question) );
    log_msg( LOG_INFO, "Sent role question to client (socket %d)", sock_client );
    
    // read role answer
    int l_len = read( sock_client, buf, sizeof(buf) - 1 );
    if ( l_len <= 0 )
    {
        log_msg( LOG_ERROR, "Failed to read role from client" );
        close( sock_client );
        pthread_exit( nullptr );
    }
    
    buf[l_len] = '\0';
    // remove newline
    if ( buf[l_len - 1] == '\n' )
        buf[l_len - 1] = '\0';
    
    log_msg( LOG_INFO, "Client (socket %d) chose role: %s", sock_client, buf );
    
    // pekar (producer)
    if ( strcmp( buf, "pekar" ) == 0 )
    {
        log_msg( LOG_INFO, "Starting pekar thread for socket %d", sock_client );
        
        while (1)
        {
            // read pizza name from client
            l_len = read( sock_client, buf, sizeof(buf) - 1 );
            if ( l_len <= 0 )
            {
                log_msg( LOG_INFO, "Pekar (socket %d) disconnected", sock_client );
                break;
            }
            
            buf[l_len] = '\0';
            // remove newline
            if ( buf[l_len - 1] == '\n' )
                buf[l_len - 1] = '\0';
            
            log_msg( LOG_INFO, "Pekar sends pizza: %s", buf );
            
            // insert pizza into queue using producer()
            producer( buf );
            
            // send OK confirmation
            const char* ok_msg = "OK\n";
            write( sock_client, ok_msg, strlen(ok_msg) );
        }
    }
    // zakaznik (consumer)
    else if ( strcmp( buf, "zakaznik" ) == 0 )
    {
        log_msg( LOG_INFO, "Starting zakaznik thread for socket %d", sock_client );
        
        while (1)
        {
            char pizza_name[MAX_PIZZA_NAME];
            
            // get pizza from queue using consumer()
            consumer( pizza_name );
            
            log_msg( LOG_INFO, "Zakaznik receives pizza: %s", pizza_name );
            
            // send pizza name to client
            char pizza_msg[MAX_PIZZA_NAME + 2];
            sprintf( pizza_msg, "%s\n", pizza_name );
            write( sock_client, pizza_msg, strlen(pizza_msg) );
            
            // wait for OK confirmation from client
            l_len = read( sock_client, buf, sizeof(buf) - 1 );
            if ( l_len <= 0 )
            {
                log_msg( LOG_INFO, "Zakaznik (socket %d) disconnected", sock_client );
                break;
            }
            
            buf[l_len] = '\0';
            log_msg( LOG_DEBUG, "Zakaznik sent confirmation: %s", buf );
        }
    }
    else
    {
        log_msg( LOG_ERROR, "Unknown role: %s", buf );
    }
    
    close( sock_client );
    log_msg( LOG_INFO, "Client thread finished (socket %d)", sock_client );
    pthread_exit( nullptr );
}

//***************************************************************************
// help

void help(int t_narg, char **t_args)
{
    if (t_narg <= 1 || !strcmp( t_args[1], "-h" ))
    {
        printf(
            "\n"
            "  Pizza Server - Producer-Consumer problem.\n"
            "\n"
            "  Use: %s [-d -h] port_number\n"
            "\n"
            "    -h  this help\n"
            "    -d  debug mode \n"
            "\n", t_args[0]
        );
        exit(0);
    }

    if ( !strcmp( t_args[1], "-d" ) )
        g_debug = LOG_DEBUG;
}

//***************************************************************************

int main(int t_narg, char **t_args)
{
    if ( t_narg <= 1 )
        help(t_narg, t_args);

    log_msg( LOG_INFO, "Pizza Server starting..." );

    // port number from user
    int l_port = 0;

    // parsing arguments
    for ( int i = 1; i < t_narg; i++ )
    {
        if ( !strcmp( t_args[ i ], "-d" ) )
            g_debug = LOG_DEBUG;

        if ( !strcmp( t_args[ i ], "-h" ) )
            help( t_narg, t_args );

        if ( *t_args[ i ] != '-' && !l_port )
        {
            l_port = atoi( t_args[ i ] );
            break;
        }
    }

    if ( l_port <= 0 )
    {
        log_msg( LOG_INFO, "Bad or missing port number %d!", l_port );
        help( t_narg, t_args );
    }

    //***************************************************************
    // clean old semaphores first
    
    sem_unlink( SEM_MUTEX_NAME );
    sem_unlink( SEM_EMPTY_NAME );
    sem_unlink( SEM_FULL_NAME );

    //***************************************************************
    // create semaphores
    
    log_msg( LOG_INFO, "Creating semaphores..." );

    // semaphore mutex = 1
    g_sem_mutex = sem_open( SEM_MUTEX_NAME, O_RDWR | O_CREAT, 0660, 1 );
    if ( !g_sem_mutex )
    {
        log_msg( LOG_ERROR, "Unable to create mutex semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created mutex semaphore (initial value = 1)" );

    // semaphore empty = N
    g_sem_empty = sem_open( SEM_EMPTY_NAME, O_RDWR | O_CREAT, 0660, N );
    if ( !g_sem_empty )
    {
        log_msg( LOG_ERROR, "Unable to create empty semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created empty semaphore (initial value = N = %d)", N );

    // semaphore full = 0
    g_sem_full = sem_open( SEM_FULL_NAME, O_RDWR | O_CREAT, 0660, 0 );
    if ( !g_sem_full )
    {
        log_msg( LOG_ERROR, "Unable to create full semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created full semaphore (initial value = 0)" );

    //***************************************************************
    // initialize shared queue
    
    g_queue.in = 0;
    g_queue.out = 0;
    for ( int i = 0; i < N; i++ )
    {
        g_queue.buffer[i][0] = '\0';
    }
    
    log_msg( LOG_INFO, "Pizza queue initialized (capacity = %d)", N );

    //***************************************************************
    // setup signal handlers
    
    struct sigaction l_sa;
    bzero( &l_sa, sizeof( l_sa ) );
    l_sa.sa_handler = catch_sig;
    sigemptyset( &l_sa.sa_mask );
    l_sa.sa_flags = 0;

    // catch sig <CTRL-C>
    sigaction( SIGINT, &l_sa, nullptr );
    // catch SIG_PIPE
    sigaction( SIGPIPE, &l_sa, nullptr );

    // clean at exit
    atexit(clean);

    //***************************************************************
    // create server socket

    log_msg( LOG_INFO, "Server will listen on port: %d", l_port );

    // socket creation
    int l_sock_listen = socket( AF_INET, SOCK_STREAM, 0 );
    if ( l_sock_listen == -1 )
    {
        log_msg( LOG_ERROR, "Unable to create socket.");
        exit(1);
    }

    in_addr l_addr_any = { INADDR_ANY };
    sockaddr_in l_srv_addr;
    l_srv_addr.sin_family = AF_INET;
    l_srv_addr.sin_port = htons( l_port );
    l_srv_addr.sin_addr = l_addr_any;

    // enable the port number reusing
    int l_opt = 1;
    if ( setsockopt( l_sock_listen, SOL_SOCKET, SO_REUSEADDR, &l_opt, sizeof( l_opt ) ) < 0 )
      log_msg( LOG_ERROR, "Unable to set socket option!" );

    // assign (bind) port number to socket
    if ( bind( l_sock_listen, (const sockaddr * ) &l_srv_addr, sizeof( l_srv_addr ) ) < 0 )
    {
        log_msg( LOG_ERROR, "Bind failed!" );
        close( l_sock_listen );
        exit( 1 );
    }

    // listening on set port
    if ( listen( l_sock_listen, 10 ) < 0 )
    {
        log_msg( LOG_ERROR, "Unable to listen on given port!" );
        close( l_sock_listen );
        exit( 1 );
    }

    log_msg( LOG_INFO, "Server started. Waiting for clients..." );

    //***************************************************************
    // main loop - accept clients

    while (1)
    {
        sockaddr_in l_rsa;
        int l_rsa_size = sizeof( l_rsa );

        // accept new client
        int new_sock = accept( l_sock_listen, (sockaddr *) &l_rsa, (socklen_t *) &l_rsa_size );

        if ( new_sock == -1 )
        {
            log_msg( LOG_ERROR, "Unable to accept new client." );
            continue;
        }

        log_msg( LOG_INFO, "New client connected from %s:%d", 
                 inet_ntoa( l_rsa.sin_addr ), ntohs( l_rsa.sin_port ) );

        // create thread for new client
        pthread_t thread_id;
        int* sock_ptr = new int( new_sock );
        
        int err = pthread_create( &thread_id, nullptr, client_thread, (void*)sock_ptr );
        if ( err )
        {
            log_msg( LOG_ERROR, "Unable to create thread for client!" );
            close( new_sock );
            delete sock_ptr;
        }
        else
        {
            pthread_detach( thread_id );
            log_msg( LOG_DEBUG, "Thread created for client (socket %d)", new_sock );
        }
    }

    close( l_sock_listen );
    return 0;
}
