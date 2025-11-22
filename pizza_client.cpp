//***************************************************************************
//
// Pizza Client - role je pridelena serverem (lichy=pekar, sudy=zakaznik)
// Pekar GENERUJE pizzy a posila je serveru
// Zakaznik PRIJIMA pizzy od serveru
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
#include <netdb.h>
#include <time.h>
#include <sys/wait.h>

//***************************************************************************
// log messages

#define LOG_ERROR               0       // errors
#define LOG_INFO                1       // information and notifications
#define LOG_DEBUG               2       // debug messages

// debug flag
int g_debug = LOG_INFO;

void log_msg( int t_log_level, const char *t_form, ... )
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
// global socket
int g_sock_server = -1;

//***************************************************************************
// pekar process - GENERUJE pizzy a posila je serveru

void pekar_process( void )
{
    log_msg( LOG_INFO, "Pekar process started (PID %d)", getpid() );
    
    const char* pizza_names[] = 
    {
        "Margherita",
        "Prosciutto", 
        "Capricciosa",
        "Quattro Formaggi",
        "Diavola",
        "Hawaii",
        "Marinara",
        "Funghi"
    };
    int num_pizzas = sizeof(pizza_names) / sizeof(pizza_names[0]);
    
    while (1)
    {
        // wait 5 seconds before producing next pizza
        sleep(5);
        
        // select random pizza
        int pizza_idx = rand() % num_pizzas;
        const char* pizza_name = pizza_names[pizza_idx];
        
        log_msg( LOG_INFO, "Pekar produces pizza: %s", pizza_name );
        
        // send pizza name to server
        char pizza_msg[128];
        sprintf( pizza_msg, "%s\n", pizza_name );
        int l_written = write( g_sock_server, pizza_msg, strlen(pizza_msg) );
        if ( l_written <= 0 )
        {
            log_msg( LOG_INFO, "Server disconnected" );
            break;
        }
        
        // wait for OK confirmation from server
        char buf[128];
        int l_len = read( g_sock_server, buf, sizeof(buf) - 1 );
        if ( l_len <= 0 )
        {
            log_msg( LOG_INFO, "Server disconnected" );
            break;
        }
        
        buf[l_len] = '\0';
        log_msg( LOG_DEBUG, "Received confirmation: %s", buf );

        fflush(stdout);
    }
    
    log_msg( LOG_INFO, "Pekar process finished (PID %d)", getpid() );
}

//***************************************************************************
// zakaznik process - PRIJIMA pizzy od serveru (cela prepravka)

void zakaznik_process( void )
{
    log_msg( LOG_INFO, "Zakaznik process started (PID %d)", getpid() );
    
    while (1)
    {
        char buf[1024];
        
        // read prepravka info from server
        int l_len = read( g_sock_server, buf, sizeof(buf) - 1 );
        if ( l_len <= 0 )
        {
            log_msg( LOG_INFO, "Server disconnected" );
            break;
        }
        
        buf[l_len] = '\0';
        printf( "%s", buf );
        fflush(stdout);
    }
    
    log_msg( LOG_INFO, "Zakaznik process finished (PID %d)", getpid() );
}

//***************************************************************************
// help

void help( int t_narg, char **t_args )
{
    if ( t_narg <= 1 || !strcmp( t_args[ 1 ], "-h" ) )
    {
        printf(
            "\n"
            "  Pizza client.\n"
            "  Role je pridelena serverem (lichy=pekar, sudy=zakaznik).\n"
            "  Pekar generuje pizzy co 5 sekund a posila je serveru.\n"
            "  Zakaznik prijima prepravky pizz od serveru.\n"
            "\n"
            "  Use: %s [-h -d] ip_or_name port_number\n"
            "\n"
            "    -d  debug mode \n"
            "    -h  this help\n"
            "\n", t_args[ 0 ] );

        exit( 0 );
    }

    if ( !strcmp( t_args[ 1 ], "-d" ) )
        g_debug = LOG_DEBUG;
}

//***************************************************************************

int main( int t_narg, char **t_args )
{
    srand(time(NULL) ^ getpid());  // different seed for each client

    if ( t_narg <= 2 )
        help( t_narg, t_args );

    // port number from user
    int l_port = 0;
    char *l_host = nullptr;

    // parsing arguments
    for ( int i = 1; i < t_narg; i++ )
    {
        if ( !strcmp( t_args[ i ], "-d" ) )
            g_debug = LOG_DEBUG;

        if ( !strcmp( t_args[ i ], "-h" ) )
            help( t_narg, t_args );

        if ( *t_args[ i ] != '-' )
        {
            if ( !l_host )
                l_host = t_args[ i ];
            else if ( !l_port )
                l_port = atoi( t_args[ i ] );
        }
    }

    if ( !l_host || !l_port )
    {
        log_msg( LOG_INFO, "Host or port is missing!" );
        help( t_narg, t_args );
        exit( 1 );
    }

    log_msg( LOG_INFO, "Connection to '%s':%d.", l_host, l_port );

    addrinfo l_ai_req;
    addrinfo *l_ai_ans;
    bzero( &l_ai_req, sizeof( l_ai_req ) );
    l_ai_req.ai_family = AF_INET;
    l_ai_req.ai_socktype = SOCK_STREAM;

    int l_get_ai = getaddrinfo( l_host, nullptr, &l_ai_req, &l_ai_ans );
    if ( l_get_ai )
    {
        log_msg( LOG_ERROR, "Unknown host name!" );
        exit( 1 );
    }

    sockaddr_in l_cl_addr =  *( sockaddr_in * ) l_ai_ans->ai_addr;
    l_cl_addr.sin_port = htons( l_port );
    freeaddrinfo( l_ai_ans );

    // socket creation
    g_sock_server = socket( AF_INET, SOCK_STREAM, 0 );
    if ( g_sock_server == -1 )
    {
        log_msg( LOG_ERROR, "Unable to create socket.");
        exit( 1 );
    }

    // connect to server
    if ( connect( g_sock_server, ( sockaddr * ) &l_cl_addr, sizeof( l_cl_addr ) ) < 0 )
    {
        log_msg( LOG_ERROR, "Unable to connect server." );
        exit( 1 );
    }

    log_msg( LOG_INFO, "Connected to server" );

    // read assigned role from server
    char buf[128];
    int l_len = read( g_sock_server, buf, sizeof(buf) - 1 );
    if ( l_len <= 0 )
    {
        log_msg( LOG_ERROR, "Failed to read from server" );
        exit( 1 );
    }
    
    buf[l_len] = '\0';
    printf( "%s", buf );  // print "Role: pekar\n" or "Role: zakaznik\n"
    
    // parse role from server response
    char role[32] = {0};
    if ( sscanf( buf, "Role: %31s", role ) != 1 )
    {
        log_msg( LOG_ERROR, "Failed to parse role from server" );
        exit( 1 );
    }
    
    // remove newline from role if present
    role[strcspn(role, "\n")] = 0;
    
    log_msg( LOG_INFO, "Server assigned role: %s", role );
    
    // start appropriate process based on assigned role
    if ( strcmp( role, "pekar" ) == 0 )
    {
        pekar_process();
    }
    else if ( strcmp( role, "zakaznik" ) == 0 )
    {
        zakaznik_process();
    }
    else
    {
        log_msg( LOG_ERROR, "Unknown role: %s", role );
    }

    close( g_sock_server );
    return 0;
}
