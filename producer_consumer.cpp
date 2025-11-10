//***************************************************************************
//
// This program uses 3 POSIX semaphores (mutex, empty, full) to solve
// the producer-consumer synchronization problem with a circular buffer.
//
//***************************************************************************

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <semaphore.h>

//***************************************************************************
// constants

#define N 100               // number of slots in the buffer

#define SEM_MUTEX_NAME      "/sem_mutex"
#define SEM_EMPTY_NAME      "/sem_empty"
#define SEM_FULL_NAME       "/sem_full"

//***************************************************************************
// shared memory structure

struct shared_data 
{
    int buffer[N];       // the buffer
    int in;              // index for producer
    int out;             // index for consumer
};

//***************************************************************************
// global variables

sem_t *g_sem_mutex = nullptr;    // controls access to critical region
sem_t *g_sem_empty = nullptr;    // counts empty buffer slots
sem_t *g_sem_full = nullptr;     // counts full buffer slots

struct shared_data *g_shared = nullptr;  // pointer to shared memory
int g_shmid = -1;                        // shared memory ID

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
// cleaning function

void clean(void)
{
    log_msg(LOG_INFO, "Final cleaning ...");

    // detach shared memory
    if (g_shared)
    {
        shmdt(g_shared);
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

    // remove shared memory
    if (g_shmid >= 0)
    {
        shmctl(g_shmid, IPC_RMID, nullptr);
    }
}

// catch signal = for example CTRL+C sig (signal)
void catch_sig(int t_sig)
{
  exit(1);
}

//***************************************************************************
// help

void help(int t_narg, char **t_args)
{
    if (t_narg <= 1) return; // no arguments, go back

    if ( !strcmp( t_args[1], "-h" ) )
    {
        printf(
            "\n"
            "  Producer-Consumer problem example.\n"
            "\n"
            "  Use: %s [-d -h -r]\n"
            "\n"
            "    -h  this help\n"
            "    -d  debug mode \n"
            "    -r  clean semaphores \n"
            "\n", t_args[0]
        );
        exit(0);
    }

    if ( !strcmp( t_args[1], "-d" ) )
        g_debug = LOG_DEBUG;

    if ( !strcmp( t_args[1], "-r" ) )
    {
        log_msg( LOG_INFO, "Clean semaphores." );
        sem_unlink( SEM_MUTEX_NAME );
        sem_unlink( SEM_EMPTY_NAME );
        sem_unlink( SEM_FULL_NAME );
        exit(0);
    }
}

//***************************************************************************
// Helper functions from Figure 2-28

int produce_item( void )
{
    static int item = 0;
    item++;
    usleep( 100000 );  /* simulate work */
    return item;
}

void insert_item( int item )
{
    g_shared->buffer[ g_shared->in ] = item;
    g_shared->in = ( g_shared->in + 1 ) % N;
}

int remove_item( void )
{
    int item = g_shared->buffer[ g_shared->out ];
    g_shared->out = ( g_shared->out + 1 ) % N;
    return item;
}

void consume_item( int item )
{
    usleep( 150000 );  /* simulate work */
}

//***************************************************************************
// PRODUCER function from Figure 2-28

void producer( void )
{
    int item;
    
    log_msg( LOG_INFO, "Producer started (PID %d)", getpid() );

    for ( int i = 0; i < 20; i++ )  /* produce 20 items */
    {
        item = produce_item();
        log_msg( LOG_DEBUG, "Produced item %d", item );
        
        /* down(&empty) */
        if ( sem_wait( g_sem_empty ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_wait(empty) failed" );
            exit( 1 );
        }
        
        /* down(&mutex) */
        if ( sem_wait( g_sem_mutex ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_wait(mutex) failed" );
            exit( 1 );
        }
        
        /* insert item into buffer */
        insert_item( item );
        log_msg( LOG_DEBUG, "Inserted item %d into buffer", item );
        
        /* up(&mutex) */
        if ( sem_post( g_sem_mutex ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(mutex) failed" );
            exit( 1 );
        }
        
        /* up(&full) */
        if ( sem_post( g_sem_full ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(full) failed" );
            exit( 1 );
        }
    }
    
    log_msg( LOG_INFO, "Producer finished" );
}

//***************************************************************************
// CONSUMER function from Figure 2-28

void consumer( void )
{
    int item;
    
    log_msg( LOG_INFO, "Consumer started (PID %d)", getpid() );

    for ( int i = 0; i < 20; i++ )  /* consume 20 items */
    {
        /* down(&full) */
        if ( sem_wait( g_sem_full ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_wait(full) failed" );
            exit( 1 );
        }
        
        /* down(&mutex) */
        if ( sem_wait( g_sem_mutex ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_wait(mutex) failed" );
            exit( 1 );
        }
        
        /* remove item from buffer */
        item = remove_item();
        log_msg( LOG_DEBUG, "Removed item %d from buffer", item );
        
        /* up(&mutex) */
        if ( sem_post( g_sem_mutex ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(mutex) failed" );
            exit( 1 );
        }
        
        /* up(&empty) */
        if ( sem_post( g_sem_empty ) < 0 )
        {
            log_msg( LOG_ERROR, "sem_post(empty) failed" );
            exit( 1 );
        }
        
        /* consume item */
        consume_item( item );
        log_msg( LOG_DEBUG, "Consumed item %d", item );
    }
    
    log_msg( LOG_INFO, "Consumer finished" );
}

//***************************************************************************

int main(int t_narg, char **t_args)
{
    help(t_narg, t_args);

    log_msg( LOG_INFO, "Producer-Consumer problem starting..." );

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
    // create shared memory
    
    log_msg(LOG_INFO, "Creating shared memory...");
    
    g_shmid = shmget( IPC_PRIVATE, sizeof(struct shared_data), IPC_CREAT | 0660 );
    if ( g_shmid < 0 )
    {
        log_msg( LOG_ERROR, "Unable to create shared memory!" );
        return 1;
    }
    
    g_shared = (struct shared_data *)shmat( g_shmid, nullptr, 0 );
    if ( g_shared == (void *)-1 )
    {
        log_msg( LOG_ERROR, "Unable to attach shared memory!" );
        return 1;
    }
    
    // initialize buffer
    g_shared->in = 0;
    g_shared->out = 0;
    for ( int i = 0; i < N; i++ )
    {
        g_shared->buffer[i] = 0;
    }
    
    log_msg( LOG_INFO, "Shared memory created and initialized" );

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
    // create producer process
    
    log_msg( LOG_INFO, "Creating producer process..." );
    
    pid_t producer_pid = fork();
    
    if ( producer_pid < 0 )
    {
        log_msg( LOG_ERROR, "fork() failed for producer" );
        return 1;
    }
    
    if ( producer_pid == 0 )
    {
        // child process - PRODUCER
        
        // open semaphores (they already exist)
        g_sem_mutex = sem_open( SEM_MUTEX_NAME, O_RDWR );
        g_sem_empty = sem_open( SEM_EMPTY_NAME, O_RDWR );
        g_sem_full = sem_open( SEM_FULL_NAME, O_RDWR );
        
        // attach to existing shared memory
        g_shared = (struct shared_data *)shmat( g_shmid, nullptr, 0 );
        
        // run producer
        producer();
        
        // detach from shared memory
        shmdt( g_shared );
        
        // child exits
        exit(0);
    }

    //***************************************************************
    // create consumer process
    
    log_msg( LOG_INFO, "Creating consumer process..." );
    
    pid_t consumer_pid = fork();
    
    if ( consumer_pid < 0 )
    {
        log_msg( LOG_ERROR, "fork() failed for consumer" );
        return 1;
    }
    
    if ( consumer_pid == 0 )
    {
        // child process - CONSUMER
        
        // open semaphores (they already exist)
        g_sem_mutex = sem_open( SEM_MUTEX_NAME, O_RDWR );
        g_sem_empty = sem_open( SEM_EMPTY_NAME, O_RDWR );
        g_sem_full = sem_open( SEM_FULL_NAME, O_RDWR );
        
        // attach to existing shared memory
        g_shared = (struct shared_data *)shmat( g_shmid, nullptr, 0 );
        
        // run consumer
        consumer();
        
        // detach from shared memory
        shmdt(g_shared);
        
        // child exits
        exit(0);
    }

    //***************************************************************
    // parent waits for children
    
    log_msg( LOG_INFO, "Parent waiting for children to finish..." );

    // Wait for all
    // waitpid(-1, nullptr, 0);
    
    // wait for producer
    waitpid( producer_pid, nullptr, 0 );
    log_msg( LOG_INFO, "Producer process finished" );
    
    // wait for consumer
    waitpid( consumer_pid, nullptr, 0 );
    log_msg( LOG_INFO, "Consumer process finished" );
    
    log_msg( LOG_INFO, "All processes finished successfully!" );
    
    return 0;
}
