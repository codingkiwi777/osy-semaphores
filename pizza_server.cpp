//***************************************************************************
//
// Pizza Server - Producer-Consumer using shared memory and processes
// Buffer jako prepravka (az se naplni, odesle se zakaznikovi)
//
//***************************************************************************

/*
    # Spuštění serveru (terminál 1)
    ./pizza_server 12345

    # Spuštění klientů (terminál 2, 3, ...)
    ./pizza_client 127.0.0.1 12345
    # Server automaticky přidělí roli:
    # - 1. klient (lichý) = pekar
    # - 2. klient (sudý) = zakaznik
    # - 3. klient (lichý) = pekar
    # - atd.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>

//***************************************************************************
// constants

#define N 10                        // number of slots in the pizza queue (prepravka)
#define MAX_PIZZA_NAME 64           // max pizza name length

#define SEM_MUTEX_NAME      "/sem_pizza_mutex"
#define SEM_EMPTY_NAME      "/sem_pizza_empty"
#define SEM_FULL_NAME       "/sem_pizza_full"

#define SHM_NAME            "/shm_pizza_queue"

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
// shared data structure - pizza queue in shared memory (prepravka)

struct pizza_queue 
{
    char buffer[N][MAX_PIZZA_NAME];  // the buffer for pizza names
    int state;                       // current number of items in prepravka (0 to N)
    int item_counter;                // global counter for numbering pizzas
};

//***************************************************************************
// global variables

sem_t *g_sem_mutex = nullptr;    // controls access to critical region
sem_t *g_sem_empty = nullptr;    // binary: 1 = prepravka is not full (pekar can add)
sem_t *g_sem_full = nullptr;     // binary: 1 = prepravka is full (zakaznik can take)

struct pizza_queue *g_queue = nullptr;  // pointer to shared memory queue
int g_shm_fd = -1;                      // shared memory file descriptor

//***************************************************************************
// insert_item function - inserts pizza into prepravka with numbering

void insert_item( char *pizza )
{
    /* down(&empty) - wait until prepravka is not full */
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
    
    /* insert pizza into buffer with numbering: "N. item" */
    g_queue->item_counter++;
    sprintf( g_queue->buffer[ g_queue->state ], "%d. %s", g_queue->item_counter, pizza );
    log_msg( LOG_DEBUG, "Inserted pizza '%s' into prepravka at position %d", 
             g_queue->buffer[ g_queue->state ], g_queue->state );
    g_queue->state++;
    
    /* up(&mutex) */
    if ( sem_post( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(mutex) failed" );
        return;
    }
    
    /* if prepravka is full, signal zakaznik, else signal pekar can continue */
    if ( g_queue->state == N )
    {
        /* up(&full) - prepravka is full, zakaznik can take it */
        if ( sem_post( g_sem_full ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(full) failed" );
            return;
        }
    }
    else
    {
        /* up(&empty) - prepravka is not full yet, pekar can continue */
        if ( sem_post( g_sem_empty ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(empty) failed" );
            return;
        }
    }
}

//***************************************************************************
// remove_item function - removes ALL pizzas from prepravka (cela paleta)

void remove_item( char pizza_out[N][MAX_PIZZA_NAME], int *count )
{
    /* down(&full) - wait until prepravka is full */
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
    
    /* remove ALL pizzas from buffer (cela paleta) */
    *count = g_queue->state;
    for ( int i = 0; i < g_queue->state; i++ )
    {
        strncpy( pizza_out[i], g_queue->buffer[i], MAX_PIZZA_NAME - 1 );
        pizza_out[i][ MAX_PIZZA_NAME - 1 ] = '\0';
        log_msg( LOG_DEBUG, "Removed pizza '%s' from prepravka at position %d", pizza_out[i], i );
    }
    g_queue->state = 0;  // prepravka is now empty
    
    /* up(&mutex) */
    if ( sem_post( g_sem_mutex ) < 0 )
    {
        log_msg( LOG_ERROR, "sem_post(mutex) failed" );
        return;
    }
    
    /* prepravka is now empty, pekar can add */
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

    // unmap shared memory
    if (g_queue)
    {
        munmap(g_queue, sizeof(struct pizza_queue));
    }
    
    // close and unlink shared memory
    if (g_shm_fd != -1)
    {
        close(g_shm_fd);
        shm_unlink(SHM_NAME);
    }

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
// client process function

void client_process( int sock_client, int client_number )
{
    char buf[256];
    
    // open existing semaphores
    g_sem_mutex = sem_open( SEM_MUTEX_NAME, 0 );
    if ( !g_sem_mutex )
    {
        log_msg( LOG_ERROR, "Unable to open mutex semaphore in child!" );
        exit(1);
    }
    
    g_sem_empty = sem_open( SEM_EMPTY_NAME, 0 );
    if ( !g_sem_empty )
    {
        log_msg( LOG_ERROR, "Unable to open empty semaphore in child!" );
        exit(1);
    }
    
    g_sem_full = sem_open( SEM_FULL_NAME, 0 );
    if ( !g_sem_full )
    {
        log_msg( LOG_ERROR, "Unable to open full semaphore in child!" );
        exit(1);
    }
    
    // open existing shared memory
    g_shm_fd = shm_open( SHM_NAME, O_RDWR, 0660 );
    if ( g_shm_fd < 0 )
    {
        log_msg( LOG_ERROR, "Unable to open shared memory in child!" );
        exit(1);
    }
    
    // map shared memory
    g_queue = (struct pizza_queue*) mmap( nullptr, 
                                          sizeof(struct pizza_queue), 
                                          PROT_READ | PROT_WRITE, 
                                          MAP_SHARED, 
                                          g_shm_fd, 
                                          0
                                        );
    if ( g_queue == MAP_FAILED )
    {
        log_msg( LOG_ERROR, "Unable to map shared memory in child!" );
        exit(1);
    }
    
    // determine role based on client_number (lichy = pekar, sudy = zakaznik)
    const char* role;
    if ( client_number % 2 == 1 )
    {
        role = "pekar";
    }
    else
    {
        role = "zakaznik";
    }
    
    // send role to client (server assigns role, ignores client's choice)
    char role_msg[64];
    sprintf( role_msg, "Role: %s\n", role );
    write( sock_client, role_msg, strlen(role_msg) );
    log_msg( LOG_INFO, "Client #%d (socket %d, PID %d) assigned role: %s", 
             client_number, sock_client, getpid(), role );
    
    // pekar (producer) - lichy klient
    // Server PRIJIMA pizzy od klienta a uklada je do fronty
    if ( strcmp( role, "pekar" ) == 0 )
    {
        log_msg( LOG_INFO, "Starting pekar handler (PID %d)", getpid() );
        
        while (1)
        {
            // read pizza name from client
            int l_len = read( sock_client, buf, sizeof(buf) - 1 );
            if ( l_len <= 0 )
            {
                log_msg( LOG_INFO, "Pekar (PID %d) disconnected", getpid() );
                break;
            }
            
            buf[l_len] = '\0';
            // remove newline
            buf[strcspn(buf, "\r\n")] = '\0';
            
            if ( strlen(buf) == 0 )
                continue;
            
            log_msg( LOG_INFO, "Pekar (PID %d) received pizza: %s", getpid(), buf );
            
            // insert pizza into prepravka
            insert_item( buf );
            
            // send OK confirmation to client
            const char* ok_msg = "OK\n";
            int l_written = write( sock_client, ok_msg, strlen(ok_msg) );
            if ( l_written <= 0 )
            {
                log_msg( LOG_INFO, "Pekar (PID %d) disconnected", getpid() );
                break;
            }
        }
    }
    // zakaznik (consumer) - sudy klient
    // Server POSILA pizzy klientovi z fronty
    else if ( strcmp( role, "zakaznik" ) == 0 )
    {
        log_msg( LOG_INFO, "Starting zakaznik handler (PID %d)", getpid() );
        
        while (1)
        {
            char pizzas[N][MAX_PIZZA_NAME];
            int count = 0;
            
            // get whole prepravka (cela paleta)
            remove_item( pizzas, &count );
            
            log_msg( LOG_INFO, "Zakaznik (PID %d) receives prepravka with %d pizzas", getpid(), count );
            
            // send all pizzas to client
            char msg[1024];
            sprintf( msg, "=== Prepravka (%d pizz) ===\n", count );
            write( sock_client, msg, strlen(msg) );
            
            for ( int i = 0; i < count; i++ )
            {
                sprintf( msg, "%s\n", pizzas[i] );
                int l_written = write( sock_client, msg, strlen(msg) );
                if ( l_written <= 0 )
                {
                    log_msg( LOG_INFO, "Zakaznik (PID %d) disconnected", getpid() );
                    goto cleanup;
                }
            }
            
            sprintf( msg, "=== Konec prepravky ===\n" );
            write( sock_client, msg, strlen(msg) );
        }
    }
    
cleanup:
    // cleanup
    munmap( g_queue, sizeof(struct pizza_queue) );
    close( g_shm_fd );
    sem_close( g_sem_mutex );
    sem_close( g_sem_empty );
    sem_close( g_sem_full );
    close( sock_client );
    
    log_msg( LOG_INFO, "Client process finished (PID %d)", getpid() );
    exit(0);
}

//***************************************************************************
// help

void help(int t_narg, char **t_args)
{
    if (t_narg <= 1 || !strcmp( t_args[1], "-h" ))
    {
        printf(
            "\n"
            "  Pizza Server - Producer-Consumer with shared memory.\n"
            "  Buffer jako prepravka (az se naplni, odesle se zakaznikovi).\n"
            "\n"
            "  Use: %s [-d -h -r] port_number\n"
            "\n"
            "    -h  this help\n"
            "    -d  debug mode \n"
            "    -r  clean semaphores and shared memory\n"
            "\n", t_args[0]
        );
        exit(0);
    }

    if ( !strcmp( t_args[1], "-d" ) )
        g_debug = LOG_DEBUG;
        
    if ( !strcmp( t_args[1], "-r" ) )
    {
        log_msg( LOG_INFO, "Clean semaphores and shared memory." );
        sem_unlink( SEM_MUTEX_NAME );
        sem_unlink( SEM_EMPTY_NAME );
        sem_unlink( SEM_FULL_NAME );
        shm_unlink( SHM_NAME );
        exit(0);
    }
}

//***************************************************************************

int main(int t_narg, char **t_args)
{
    if ( t_narg <= 1 )
        help(t_narg, t_args);

    log_msg( LOG_INFO, "Pizza Server starting..." );

    srand(time(NULL));

    // port number from user
    int l_port = 0;

    // parsing arguments
    for ( int i = 1; i < t_narg; i++ )
    {
        if ( !strcmp( t_args[ i ], "-d" ) )
            g_debug = LOG_DEBUG;

        if ( !strcmp( t_args[ i ], "-h" ) )
            help( t_narg, t_args );
            
        if ( !strcmp( t_args[ i ], "-r" ) )
        {
            log_msg( LOG_INFO, "Clean semaphores and shared memory." );
            sem_unlink( SEM_MUTEX_NAME );
            sem_unlink( SEM_EMPTY_NAME );
            sem_unlink( SEM_FULL_NAME );
            shm_unlink( SHM_NAME );
            exit(0);
        }

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
    // clean old semaphores and shared memory first
    
    sem_unlink( SEM_MUTEX_NAME );
    sem_unlink( SEM_EMPTY_NAME );
    sem_unlink( SEM_FULL_NAME );
    shm_unlink( SHM_NAME );

    //***************************************************************
    // create semaphores - all binary now
    
    log_msg( LOG_INFO, "Creating semaphores..." );

    // semaphore mutex = 1 (binary)
    g_sem_mutex = sem_open( SEM_MUTEX_NAME, O_RDWR | O_CREAT, 0660, 1 );
    if ( !g_sem_mutex )
    {
        log_msg( LOG_ERROR, "Unable to create mutex semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created mutex semaphore (initial value = 1)" );

    // semaphore empty = 1 (binary) - prepravka is empty, pekar can add
    g_sem_empty = sem_open( SEM_EMPTY_NAME, O_RDWR | O_CREAT, 0660, 1 );
    if ( !g_sem_empty )
    {
        log_msg( LOG_ERROR, "Unable to create empty semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created empty semaphore (initial value = 1, binary)" );

    // semaphore full = 0 (binary) - prepravka is not full yet
    g_sem_full = sem_open( SEM_FULL_NAME, O_RDWR | O_CREAT, 0660, 0 );
    if ( !g_sem_full )
    {
        log_msg( LOG_ERROR, "Unable to create full semaphore!" );
        return 1;
    }
    log_msg( LOG_INFO, "Created full semaphore (initial value = 0, binary)" );

    //***************************************************************
    // create and initialize shared memory
    
    log_msg( LOG_INFO, "Creating shared memory..." );
    
    // create shared memory object
    g_shm_fd = shm_open( SHM_NAME, O_RDWR | O_CREAT, 0660 );
    if ( g_shm_fd < 0 )
    {
        log_msg( LOG_ERROR, "Unable to create shared memory!" );
        return 1;
    }
    
    // set size of shared memory
    if ( ftruncate( g_shm_fd, sizeof(struct pizza_queue) ) < 0 )
    {
        log_msg( LOG_ERROR, "Unable to set size of shared memory!" );
        return 1;
    }
    
    // map shared memory
    g_queue = (struct pizza_queue*) mmap( nullptr, 
                                          sizeof(struct pizza_queue), 
                                          PROT_READ | PROT_WRITE, 
                                          MAP_SHARED, 
                                          g_shm_fd, 
                                          0 
                                        );
    if ( g_queue == MAP_FAILED )
    {
        log_msg( LOG_ERROR, "Unable to map shared memory!" );
        return 1;
    }
    
    log_msg( LOG_INFO, "Shared memory created and mapped" );
    
    //***************************************************************
    // initialize shared queue (prepravka)
    
    g_queue->state = 0;          // prepravka is empty
    g_queue->item_counter = 0;   // start numbering from 0
    for ( int i = 0; i < N; i++ )
    {
        g_queue->buffer[i][0] = '\0';
    }
    
    log_msg( LOG_INFO, "Prepravka initialized (capacity = %d)", N );

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
        exit( 1 );
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
    log_msg( LOG_INFO, "Role assignment: odd client = pekar, even client = zakaznik" );

    //***************************************************************
    // main loop - accept clients and create processes
    
    int g_client_counter = 0;  // counter for client numbering

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

        g_client_counter++;
        
        log_msg( LOG_INFO, "New client #%d connected from %s:%d", 
                 g_client_counter, inet_ntoa( l_rsa.sin_addr ), ntohs( l_rsa.sin_port ) );

        // fork new process for client
        pid_t pid = fork();
        
        if ( pid == 0 )
        {
            // child process
            close( l_sock_listen );  // child doesnt need listening socket
            client_process( new_sock, g_client_counter );
            exit(0);
        }
        else if ( pid > 0 )
        {
            // parent process
            close( new_sock );  // parent doesn't need client socket
            log_msg( LOG_DEBUG, "Created process (PID %d) for client #%d", pid, g_client_counter );
            
            // clean up zombie processes
            while ( waitpid( -1, nullptr, WNOHANG ) > 0 );
        }
        else
        {
            log_msg( LOG_ERROR, "Unable to fork process for client!" );
            close( new_sock );
        }
    }

    close( l_sock_listen );
    return 0;
}
