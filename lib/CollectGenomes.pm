#!/usr/bin/env perl
package CollectGenomes;

use 5.010;
use strict;
use warnings;
use autodie;
no warnings 'experimental::smartmatch';   #for when
use Exporter qw/import/;
use Carp;
use Data::Dumper;
use Path::Tiny;
use DBI;
use Getopt::Long;
use Pod::Usage;
use Capture::Tiny qw/capture/;
use Log::Log4perl;
use Net::FTP;
use Net::FTP::Robust;
use Net::FTP::AutoReconnect;
use PerlIO::gzip;
use Archive::Extract;
use POSIX qw(mkfifo);
use IO::Prompter;
use File::Find::Rule;
#use Regexp::Debugger;
use HTML::TreeBuilder;
use LWP::Simple;
use DateTime::Tiny;

our $VERSION = "0.01";

our @EXPORT_OK = qw{
	main
    init_logging
	get_parameters_from_cmd
	dbi_connect
	create_database
	capture_output
	ftp_robust
	extract_and_load_nr
    extract_and_load_gi_taxid
	ti_gi_fasta
	get_existing_ti
	import_names
	import_nodes
    get_missing_genomes
	delete_extra_genomes
	delete_full_genomes
	print_nr_genomes
	copy_existing_genomes
	ensembl_ftp
	ensembl_ftp_vertebrates
	prepare_cdhit_per_phylostrata
	run_cdhit
	
	};

#MODULINO - works with debugger too
main() if ! caller() or (caller)[0] eq 'DB';


### INTERFACE SUB starting all others ###
# Usage      : main();
# Purpose    : it starts all other subs and entire modulino
# Returns    : nothing
# Parameters : none (argument handling by Getopt::Long)
# Throws     : lots of exceptions from die
# Comments   : start of entire module
# See Also   : n/a
sub main {
    croak 'main() does not need parameters' unless @_ == 0;

	#start logging first so it captures capturing of parameters too
    init_logging();
    ##########################
    # ... in some function ...
    ##########################
    my $log = Log::Log4perl::get_logger("main");
    # Logs both to Screen and File appender
    $log->info("This is start of logging for $0");

    my ($param_href) = get_parameters_from_cmd();

    #preparation of parameters
    my @MODE     = @{ $param_href->{MODE} };
    my $NODES    = $param_href->{NODES};
    my $NAMES    = $param_href->{NAMES};
    my $BLASTDB  = $param_href->{BLASTDB};
    my $ORG      = $param_href->{ORG};
    my $OUT      = $param_href->{OUT};
    my $HOST     = $param_href->{HOST};
    my $DATABASE = $param_href->{DATABASE};
    my $USER     = $param_href->{USER};
    my $PASSWORD = $param_href->{PASSWORD};
    my $PORT     = $param_href->{PORT};
    my $SOCKET   = $param_href->{SOCKET};
    my $TAX_ID   = $param_href->{TAX_ID};

    #need to create dispatch table for different usage depending on mode requested
    #dispatch table is hash (could be also hash_ref)
    my %dispatch = (
        create_db                     => \&create_database,
        nr_ftp                        => \&ftp_robust,
        extract_nr                    => \&extract_nr,
        load_nr                       => \&load_nr,
        extract_and_load_nr           => \&extract_and_load_nr,
        gi_taxid                      => \&extract_and_load_gi_taxid,
        ti_gi_fasta                   => \&ti_gi_fasta,
        get_existing_ti               => \&get_existing_ti,
        import_names                  => \&import_names,
        import_nodes                  => \&import_nodes,
        get_missing_genomes           => \&get_missing_genomes,
        delete_extra_genomes          => \&delete_extra_genomes,
        delete_full_genomes           => \&delete_full_genomes,
        print_nr_genomes              => \&print_nr_genomes,
        copy_existing_genomes         => \&copy_existing_genomes,
        ensembl_vertebrates           => \&ensembl_ftp_vertebrates,
        ensembl_ftp                   => \&ensembl_ftp,
        prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
        run_cdhit                     => \&run_cdhit,

    );


    #start mode that is requested (reference to subroutine)
    #some modes can be combined (check Usage)
    foreach my $mode (@MODE) {    #could be more than one
        if ( exists $dispatch{$mode} ) {    #check if mode misspelled
            $log->info( "RUNNING ACTION for mode: ", $mode );
            $log->info("TIME when started for: $mode");

            $dispatch{$mode}->($param_href);

            $log->info("TIME when finished for: $mode");
        }
        else {
            #complain if mode misspelled
            $log->logcroak("Unrecognized mode --mode=$mode on command line thus aborting");
        }
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : my ($param_href) = get_parameters_from_cmd();
# Purpose    : processes parameters from command line
# Returns    : $param_href --> hash ref of all command line arguments and files
# Parameters : none -> works by argument handling by Getopt::Long
# Throws     : lots of exceptions from die
# Comments   : it starts logger at start
# See Also   : init_logging()
sub get_parameters_from_cmd {

    #start logger
    my $log = Log::Log4perl::get_logger("main");

    #print TRACE of command line arguments
    $log->trace( 'My @ARGV: {', join( "} {", @ARGV ), '}', "\n" );
	#<<< notidy
    my ($help,  $man,      @MODE,
		$NODES, $NAMES,    $BLASTDB, $ORG,      $TAX_ID, $MAP,
		$OUT,   $IN,       $OUTFILE, $INFILE,
		$REMOTE_HOST,      $REMOTE_DIR,         $REMOTE_FILE,
        $HOST,  $DATABASE, $USER,    $PASSWORD, $PORT,   $SOCKET, $CHARSET, $ENGINE,
    );
	#>>>
    my $VERBOSE = '';    #default false (silent)

    GetOptions(
        'help|h'           => \$help,
        'man|m'            => \$man,
        'mode|mo=s{1,}'    => \@MODE,          #accepts 1 or more arguments
        'nodes|no=s'       => \$NODES,
        'names|na=s'       => \$NAMES,
        'map=s'            => \$MAP,
        'blastdb|bl=s'     => \$BLASTDB,
        'organism|org=s'   => \$ORG,
        'tax_id|t=i'       => \$TAX_ID,
        'out|o=s'          => \$OUT,
        'outfile|of=s'     => \$OUTFILE,
        'in|i=s'           => \$IN,
        'infile|if=s'      => \$INFILE,
        'remote_host|rh=s' => \$REMOTE_HOST,
        'remote_dir|rd=s'  => \$REMOTE_DIR,
        'remote_file|rf=s' => \$REMOTE_FILE,
        'host|ho=s'        => \$HOST,
        'database|D=s'     => \$DATABASE,
        'user|u=s'         => \$USER,
        'password|p=s'     => \$PASSWORD,
        'port|po=i'        => \$PORT,
        'socket|S=s'       => \$SOCKET,
        'charset|c=s'      => \$CHARSET,
        'engine|en=s'      => \$ENGINE,
        'verbose|v'        => \$VERBOSE,       #flag
    ) or pod2usage( -verbose => 1 );

    $log->trace("Printing {@MODE} before");
    @MODE = split( /,/, join( ',', @MODE ) );
    $log->trace("Printing {@MODE} after");

    pod2usage( -verbose => 1 ) if $help;
    pod2usage( -verbose => 2 ) if $man;

    $log->fatal('No @MODE specified on command line') unless @MODE;
	#pod2usage( -verbose => 1 ) unless @MODE;

    if ($OUT) {
        $log->trace( 'My output path: ', path($OUT) );
        $OUT = path($OUT)->absolute->canonpath;
        $log->trace( 'My absolute output path: ', path($OUT) );
    }
    if ($IN) {
        $log->trace( 'My input path: ', path($IN) );
        $IN = path($IN)->absolute->canonpath;
        $log->trace( 'My absolute input path: ', path($IN) );
    }
    if ($OUTFILE) {
        $log->trace( 'My output file: ', path($OUTFILE) );
        $OUTFILE = path($OUTFILE)->absolute->canonpath;
        $log->trace( 'My absolute output file: ', path($OUTFILE) );
    }
    if ($INFILE) {
        $log->trace( 'My input file: ', path($INFILE) );
        $INFILE = path($INFILE)->absolute->canonpath;
        $log->trace( 'My absolute input file: ', path($INFILE) );
    }

    return (
        {   MODE        => \@MODE,
            NODES       => $NODES,
            NAMES       => $NAMES,
            BLASTDB     => $BLASTDB,
            ORG         => $ORG,
			MAP         => $MAP,
            TAX_ID      => $TAX_ID,
            OUT         => $OUT,
            OUTFILE     => $OUTFILE,
			IN          => $IN,
			INFILE      => $INFILE,
            REMOTE_HOST => $REMOTE_HOST,
            REMOTE_DIR  => $REMOTE_DIR,
            REMOTE_FILE => $REMOTE_FILE,
            HOST        => $HOST,
            DATABASE    => $DATABASE,
            USER        => $USER,
            PASSWORD    => $PASSWORD,
            PORT        => $PORT,
            SOCKET      => $SOCKET,
			CHARSET     => $CHARSET,
			ENGINE      => $ENGINE,
			VERBOSE     => $VERBOSE,
        }
    );
}


### INTERNAL UTILITY ###
# Usage      : init_logging();
# Purpose    : enables Log::Log4perl log() to Screen and File
# Returns    : nothing
# Parameters : doesn't need parameters (logfile is in same directory and same name as script -pl +log
# Throws     : croaks if it receives parameters  
# Comments   : used to setup a logging framework
# See Also   : Log::Log4perl at https://metacpan.org/pod/Log::Log4perl
sub init_logging {
    croak 'init_logging() does not need parameters' unless @_ == 0;

		#create log file in same dir where script is running
	my $dir_out      = path($0)->parent->absolute;                   #removes perl script and takes absolute path from rest of path
		#say '$dir_out:', $dir_out;
	my ($app_name) = path($0)->basename =~ m{\A(.+)\.(?:.+)\z};   #takes name of the script and removes .pl or .pm or .t
		#say '$app_name:', $app_name;
	my $logfile = path($dir_out, $app_name . '.log')->canonpath;     #combines all of above with .log
		#say '$logfile:', $logfile;
	
=for Regexes:
    # comment previous 3 lines when debugging regexes with Regexp::Debugger to disable this regex
	# and add this line instead
    my $logfile = 'collect_genomes_to_database.log'; 
	
=cut

	#colored output on windows
	my $osname = $^O;
	if ($osname eq 'MSWin32') {
		require Win32::Console::ANSI;    #require needs import
		Win32::Console::ANSI->import();
	}

	#levels:
    #TRACE, DEBUG, INFO, WARN, ERROR, FATAL
	###############################################################################
	#                              Log::Log4perl Conf                             #
	###############################################################################
    # Configuration in a string ...
    my $conf = qq(
      log4perl.category.main              = TRACE, Logfile, Screen
     
      log4perl.appender.Logfile           = Log::Log4perl::Appender::File
      log4perl.appender.Logfile.filename  = $logfile
      log4perl.appender.Logfile.mode      = append
      log4perl.appender.Logfile.autoflush = 1
      log4perl.appender.Logfile.umask     = 0022
      log4perl.appender.Logfile.header_text = INVOCATION:$0 @ARGV
      log4perl.appender.Logfile.layout    = Log::Log4perl::Layout::PatternLayout
      log4perl.appender.Logfile.layout.ConversionPattern = [%d{yyyy/MM/dd HH:mm:ss,SSS}]%m%n
     
      log4perl.appender.Screen            = Log::Log4perl::Appender::ScreenColoredLevels
      log4perl.appender.Screen.stderr     = 1
      log4perl.appender.Screen.layout     = Log::Log4perl::Layout::PatternLayout
      log4perl.appender.Screen.layout.ConversionPattern  = [%d{yyyy/MM/dd HH:mm:ss,SSS}]%m%n
    );
 
    # ... passed as a reference to init()
    Log::Log4perl::init( \$conf );

    return;
}

## INTERNAL UTILITY ###
# Usage      : dbi_connect();
# Purpose    : creates a connection to database
# Returns    : database handle
# Parameters : ( $param_href )
# Throws     : DBI errors and warnings
# Comments   : first part of database chain
# See Also   : DBI and DBD::mysql modules
sub dbi_connect {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'dbi_connect() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;
	
	#split logic for operating system
	my $osname = $^O;
	my $data_source;
    my $USER     = defined $param_href->{USER}     ? $param_href->{USER}     : 'msandbox';
    my $PASSWORD = defined $param_href->{PASSWORD} ? $param_href->{PASSWORD} : 'msandbox';
	
	if( $osname eq 'MSWin32' ) {	  
		my $HOST     = defined $param_href->{HOST}     ? $param_href->{HOST}     : 'localhost';
    	my $DATABASE = defined $param_href->{DATABASE} ? $param_href->{DATABASE} : 'blastdb';
    	my $PORT     = defined $param_href->{PORT}     ? $param_href->{PORT}     : 3306;
    	my $prepare  = 1;   #server side prepare is ON
		my $use_res  = 0;   #1 doesn't work with selectall_aref (O means it catches in application)

    	$data_source = "DBI:mysql:database=$DATABASE;host=$HOST;port=$PORT;mysql_server_prepare=$prepare;mysql_use_result=$use_res";
	}
	elsif ( $osname eq 'linux' ) {
		my $HOST     = defined $param_href->{HOST}     ? $param_href->{HOST}     : 'localhost';
    	my $DATABASE = defined $param_href->{DATABASE} ? $param_href->{DATABASE} : 'blastdb';
    	my $PORT     = defined $param_href->{PORT}     ? $param_href->{PORT}     : 3306;
    	my $SOCKET   = defined $param_href->{SOCKET}   ? $param_href->{SOCKET}   : '/var/lib/mysql/mysql.sock';
    	my $prepare  = 1;   #server side prepare is ON
		my $use_res  = 0;   #1 doesn't work with selectall_aref (O means it catches in application)

    	$data_source = "DBI:mysql:database=$DATABASE;host=$HOST;port=$PORT;mysql_socket=$SOCKET;mysql_server_prepare=$prepare;mysql_use_result=$use_res";
	}
	else {
		$log->error( "Running on unsupported system" );
	}

	my %conn_attrs  = (
        RaiseError         => 1,
        PrintError         => 0,
        AutoCommit         => 1,
        ShowErrorStatement => 1,
    );
    my $dbh = DBI->connect( $data_source, $USER, $PASSWORD, \%conn_attrs );

    $log->trace( 'Report: connected to ', $data_source, ' by dbh ', $dbh );

    return $dbh;
}

### INTERFACE SUB ###
# Usage      : create_database();
# Purpose    : creates database that will hold sequences to analyze, maps and others
# Returns    : nothing
# Parameters : ( $param_href ) -> params from command line
# Throws     : croaks if wrong number of parameters
# Comments   : first sub in chain, run only once at start (it drops database)
# See Also   :
sub create_database {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak ('create_database() needs a hash_ref' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $CHARSET  = defined $param_href->{CHARSET}     ? $param_href->{CHARSET}     : 'ascii';
	#repackage parameters to connect to MySQL to default mysql database and create blastdb db
    my $DATABASE = $param_href->{DATABASE};   #pull out to use here
    $param_href->{DATABASE} = 'mysql';        #insert into $param_href for dbi_connect()

    my $dbh = dbi_connect( $param_href );

    #first report what are you doing
    $log->info( "---------->$DATABASE database creation with CHARSET $CHARSET" );

    #use $DATABASE from command line
    my $drop_db_query = qq{
    DROP DATABASE IF EXISTS $DATABASE
    };
    eval { $dbh->do($drop_db_query) };
    $log->debug( "Dropping $DATABASE failed: $@" ) if $@;
    $log->debug( "Database $DATABASE dropped successfully!" ) unless $@;

    my $create_db_query = qq{
    CREATE DATABASE IF NOT EXISTS $DATABASE DEFAULT CHARSET $CHARSET
    };
    eval { $dbh->do($create_db_query) };
    $log->debug( "Creating $DATABASE failed: $@" ) if $@;
    $log->debug( "Database $DATABASE created successfully!" ) unless $@;

    return;
}

### INTERNAL UTILITY ###
# Usage      : my ($stdout, $stderr, $exit) = capture_output( $cmd, $param_href );
# Purpose    : accepts command, executes it, captures output and returns it in vars
# Returns    : STDOUT, STDERR and EXIT as vars
# Parameters : ($cmd_to_execute)
# Throws     : 
# Comments   : second param is verbose flag (default off)
# See Also   :
sub capture_output {
    my $log = Log::Log4perl::get_logger("main");
    $log->logdie( 'capture_output() needs a $cmd' ) unless (@_ ==  2 or 1);
    my ($cmd, $param_href) = @_;

    my $VERBOSE = defined $param_href->{VERBOSE}  ? $param_href->{VERBOSE}  : undef;   #default is silent
    $log->info(qq|Report: COMMAND is: $cmd|);

    my ( $stdout, $stderr, $exit ) = capture {
        system($cmd );
    };

    if ($VERBOSE) {
        $log->trace( 'STDOUT is: ', "$stdout", "\n", 'STDERR  is: ', "$stderr", "\n", 'EXIT   is: ', "$exit" );
    }

    return  $stdout, $stderr, $exit;
}


### INTERFACE SUB ###
# Usage      : ensembl_ftp_vertebrates( $param_href );
# Purpose    : downloads Ensembl vertebrates
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it downloads a
# See Also   : 
sub ensembl_ftp_vertebrates {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'ensembl_ftp_vertebrates() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );

	#part I: getting species info (species_dir and tax_id)
		#get new handle
	    my $dbh = dbi_connect($param_href);
		#select table species_ensembl_divisions to insert into
	    my $select_tables = qq{
	    SELECT TABLE_NAME 
	    FROM INFORMATION_SCHEMA.TABLES
	    WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME LIKE 'species_%'
	    };
	    my @tables = map { $_->[0] } @{ $dbh->selectall_arrayref($select_tables) };
	
	    my $table_ensembl = prompt 'Choose which SPECIES_ENSEMBL_DIVISIONS table you want to use ',
	      -menu => [ @tables ],
		  -number,
	      '>';
	    $log->trace( "Report: using $table_ensembl as a base table for copy" );
	
		#drop and create new summary table because all can get wrong
		(my $table_ensembl_end = $table_ensembl) =~ s/\d++//g;   #make a copy
	    my $drop_query = qq{
	    DROP TABLE IF EXISTS $table_ensembl_end
	    };
	    eval { $dbh->do($drop_query) };
	    $log->debug( "Action: dropping $table_ensembl_end failed: $@" ) if $@;
	    $log->debug( "Action: table $table_ensembl_end dropped successfully!" ) unless $@;
	
	    my $create_query = qq{
	    CREATE TABLE $table_ensembl_end LIKE $table_ensembl
	    };
	    eval { $dbh->do($create_query) };
	    $log->debug( "Action: creating $table_ensembl_end failed: $@" ) if $@;
	    $log->debug( "Action: table $table_ensembl_end created successfully!" ) unless $@;
	
	    my $insert_q = qq{
	    INSERT INTO $table_ensembl_end
		SELECT * FROM $table_ensembl
	    };
		my $rows;
	    eval { $rows = $dbh->do($insert_q) };
	    $log->debug( "Action: inserting into $table_ensembl_end failed: $@" ) if $@;
	    $log->debug( "Action: table $table_ensembl_end inserted $rows rows" ) unless $@;
	
		#get column names from insert table for insert later
	    my $select_columns = qq{
	        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
	        WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME = '$table_ensembl_end'
			};
	    my @columns = map { $_->[0] } @{ $dbh->selectall_arrayref($select_columns) };
	    $log->trace( "Report: using $table_ensembl_end with columns: @columns" );
	
		#extract species info from site (embedded table)
		my $tree = HTML::TreeBuilder->new; # empty tree
		$tree->parse_content(get "http://www.ensembl.org/info/about/species.html");
	
		my ($table) = $tree->look_down(_tag => q{table});   #entire table
		#say $table->as_text;
		#say $table->dump;
	
		#header has th tags
		my @headers = $table->look_down(_tag => q{th});     #column names
		@headers = map { $_->as_text } @headers;            #print as text
		say join("\t", @headers);
		my $fieldlist = join ", ", @columns;           #not all columns match existing cols
		my $field_placeholders = join ", ", map {'?'} @columns;
	
	
		#prepare insert
		my $insert_query = sprintf(qq{
			INSERT INTO %s ( $fieldlist )
	        VALUES ( $field_placeholders )
			}, $dbh->quote_identifier($table_ensembl_end) );
	    my $sth = $dbh->prepare($insert_query);
		#say $insert_query;
	
	    #rows have tr tags
	    my @rows = $table->look_down( _tag => q{tr} );      #rows in table
	    #extract rows tab-separated
	    my @col_loh;                                        #list hash refs of rows
		ROW:
	    foreach my $row (@rows) {
	        #say $row->as_text;
	        #say $row->as_HTML;
	        #say $row->dump;
	        my @cols = $row->look_down( _tag => q{td} );    #td tag splits columns
	        @cols = map { $_->as_text } @cols;              #print as text
	        say join( "\t", @cols );
			next ROW if ! defined $cols[1];
			(my $species_dir = $cols[1]) =~ s/\s+/_/g;       #scientific name -> species_dir transformation
			$species_dir = lc $species_dir;

			#mistakes in naming (from Ensembl side)
			if ($species_dir =~ m/gorilla/g) {
				$species_dir = 'gorilla_gorilla';
			}
			if ($species_dir =~ m/canis_lupus/g) {
				$species_dir = 'canis_familiaris';
			}

	        push @col_loh,
	          { species_name       => $cols[1],
				species            => $species_dir,
				division           => 'EnsemblVertebrates',
	            ti                 => $cols[2],
	            assembly           => $cols[3],
	            assembly_accession => $cols[4],
	            variation          => $cols[5],
				pan_compara        => 'N',
		        peptide_compara    => 'N',
		        genome_alignments  => 'N',
		        other_alignments   => 'N',
				core_db            => '',
	            species_id         => '0',
	        	invis              => '',
	          };
	    }
	
		#hash slice - values come in order of columns list
		INSERT:
		foreach (@col_loh) {
			eval { $sth->execute( @{$_}{@columns} ) };
			if ($@) {
				my $species_error = $_->{species_name};
				$log->error(qq|Report: insert failed for:$species_error (duplicate genome with PRE?)|);
				#say $@;
				next INSERT;
			}
			#sth->execute( $_->{species_name}, $_->{ti}, $_->{assembly}, $_->{assembly_accession}, $_->{variation} );
		}


	my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensembl.org';
	my $REMOTE_DIR  = $param_href->{REMOTE_DIR}  //= 'pub/current_fasta/';
	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );

        #part II: ftp download of vertebrate genomes
        #file for statistics and header information (print outside $OUT so you don't mix with genomes)

        my $ftp;
        $ftp = Net::FTP::AutoReconnect->new( $REMOTE_HOST, Debug => 0 )
          or $log->logdie("Action: Can't connect to $REMOTE_HOST: $@");
        $ftp->login( "anonymous", 'msestak@irb.hr' ) or $log->logdie( "Action: Can't login ", $ftp->message );
        $ftp->binary() or $log->logdie("Opening binary mode data connection failed for $_: $@");
        $ftp->cwd($REMOTE_DIR) or $log->logdie( "Can't change working directory ", $ftp->message );
        $ftp->pasv() or $log->logdie("Opening passive mode data connection failed for $_: $@");
        $log->trace( "Report: location: ", $ftp->pwd() );

        my @species_listing = $ftp->ls;
        DIR:
        foreach my $species_dir_out (@species_listing) {
            if ( $species_dir_out eq 'ancestral_alleles' ) {
                $log->trace("Action: ancestral_alleles skipped");
                next DIR;
            }

            #crucial to send $ftp to the sub (else it uses old one from previous division)
            ftp_get_proteome(
                { DIR => $species_dir_out, FTP => $ftp, TABLE => $table_ensembl_end, DBH => $dbh, %{$param_href} } );
        }

	return;
}

### INTERNAL UTILITY ###
# Usage      : ftp_get_proteome(
#            : { DIR => $species_dir_out, FTP => $ftp, TABLE => $table_ensembl_end, DBH => $dbh, %{$param_href} } );
# Purpose    : downloads proteomes
# Returns    : nothing
# Parameters : hash_ref
# Throws     : 
# Comments   : it reconnects - usable for long downloads
# See Also   : calling sub (mode) ensembl_vertebrates()
sub ftp_get_proteome {
	my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'ftp_file() needs a $param_href' ) unless @_ == 1;

    my ($param_href) = @_;

    my $OUT         = $param_href->{OUT}   or $log->logcroak('no $OUT specified on command line!');
    my $species_dir = $param_href->{DIR}   or $log->logcroak('no $species_dir sent to ftp_get_proteome!');
    my $ftp         = $param_href->{FTP}   or $log->logcroak('no $ftp sent to ftp_get_proteome!');
    my $table_info  = $param_href->{TABLE} or $log->logcroak('no $table_info sent to ftp_get_proteome!');
    my $dbh         = $param_href->{DBH}   or $log->logcroak('no $dbh sent to ftp_get_proteome!');
    my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensembl.org';
    my $REMOTE_DIR  = $param_href->{REMOTE_DIR}  //= 'pub/current_fasta/';

	$log->trace("Action: working with $species_dir" );
	$log->trace("Report: location: ", $ftp->pwd() );
	
	my $spec_path = path($species_dir, 'pep');
	$ftp->cwd($spec_path) or $log->logdie( qq|Can't change working directory to $spec_path:|, $ftp->message );
	
	#get taxid based on name of species_dir (for final output file)
	my $get_taxid_query = sprintf(qq{
	SELECT ti
	FROM %s
	WHERE species = ?}, $dbh->quote_identifier($table_info) );
	my $sth = $dbh->prepare($get_taxid_query);
	$sth->execute($species_dir);
	my $tax_id;
	$sth->bind_col(1, \$tax_id, {TYPE => 'integer'} );
	$sth->fetchrow_arrayref();

	if (!$tax_id) {
		#some entries (dirs) are not present in info file (like D. melanogaster
		#found in ensembl.org and not ensembl.genomes.org (but found on ftp)
		$log->error( qq|Report: $species_dir not found in $table_info (not found in info files (probably dleted because of TAXID problems))| );
		$ftp->cdup();   #go 2 dirs up to $REMOTE_DIR (cdup = current dir up)
		$ftp->cdup();   #cwd goes only down or cwd(..) goes to parent (cwd() goes to root)
		return;
	}


	#get fasta file inside
	my @pep_listing = $ftp->ls;
	FILE:
	foreach my $proteome (@pep_listing) {
	    next FILE unless $proteome =~ m/pep.all.fa.gz\z/;
		my $local_file = path($OUT, $proteome);

		#delete gzip file if it exists
		if (-f $local_file) {
			unlink $local_file and $log->warn( "Action: unlinked $local_file" );
		}

		#opens a filehandle to $OUT dir and downloads file there
		#Net::FTP get(REMOTE_FILE, LOCAL_FILE) accepts filehandle as LOCAL_FILE.
		open my $local_fh, ">", $local_file or $log->logdie( "Can't write to $local_file:$!" );
		$ftp->get($proteome, $local_fh) and $log->info( "Action: download to $local_file" );

		#print stats and go up 2 dirs
        my $stat_file = path($OUT)->parent;
        $stat_file = path( $stat_file, 'statistics_ensembl_all.txt' )->canonpath;
        if ( -f $stat_file ) {
            $log->warn("Action: STAT file:$stat_file already exists: appending");
        }
        open my $stat_fh, '>>', $stat_file or die "can't open file: $!";
		print {$stat_fh} path($REMOTE_HOST, $REMOTE_DIR, $spec_path, $proteome), "\t", $local_file, "\t";
		$ftp->cdup();   #go 2 dirs up to $REMOTE_DIR (cdup = current dir up)
		$ftp->cdup();   #cwd goes only down or cwd(..) goes to parent (cwd() goes to root)

		#extract file from archive
		my $ae = Archive::Extract->new( archive => "$local_file" );
		my $ae_path;
		my $ok = do {
			$ae->extract(to => $OUT) or $log->logdie( $ae->error );
			my $ae_file = $ae->files->[0];
			$ae_path = path($OUT, $ae_file);
			$log->info( "Action: extracted to $ae_path" );
		};
		#delete gziped file
		unlink $local_file and $log->trace( qq|Action: unlinked $local_file| );


		#BLOCK for writing proteomes to taxid file
		{
			open my $extracted_fh, "<", $ae_path  or $log->logdie( "Can't open $ae_path: $!" );
			my $path_taxid = path($OUT, $tax_id);
	    	open my $genome_ti_fh, ">", $path_taxid or $log->logdie( "Can't write $tax_id: $!" );
	    	
	    	#return $/ value to newline for $header_first_line
	    	local $/ = "\n";
	    	my $header_first_line = <$extracted_fh>;
	    	print {$stat_fh} $header_first_line, "\t";
	    	
	    	#return to start of file
	    	seek $extracted_fh, 0, 0;

	    	#look in larger chunks between records
	    	local $/ = ">";
	    	my $line_count = 0;
	    	while (<$extracted_fh>) {
				chomp;
	    		$line_count++;
	
	    		if (m/\A([^\h]+)(?:\h+)*(?:[^\v]+)*\v(.+)/s) {
	
					my $header = $1;
	
					my $fasta_seq = $2;
	    			$fasta_seq =~ s/\R//g;  #delete multiple newlines (also forgets %+ hash)
					$fasta_seq =~ s/[+* -._]//g;
					$fasta_seq =~ s/\d+//;
					$fasta_seq = uc $fasta_seq;
	  
	    			print $genome_ti_fh ('>', $header, "\n", $fasta_seq, "\n");
				}
			}   #end while

			if ($line_count) {
				$line_count--;   #it has one line to much
				$log->debug( qq|Action: saved to $path_taxid with $line_count lines| );
				print {$stat_fh} $line_count, "\n";
			}
		}   #block writing proteomes to taxid end

		unlink $ae_path and $log->trace( qq|Action: unlinked $ae_path| );
	}   #foreach FILE end

	return;

}


### INTERNAL UTILITY ###
# Usage      : create_table( { TABLE_NAME => $table_info, DBH => $dbh, QUERY => $create_query, %{$param_href} } );
# Purpose    : it drops and creates table
# Returns    : nothing
# Parameters : 
# Throws     : errors if it fails
# Comments   : 
# See Also   : 
sub create_table {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('create_table() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $table_name   = $param_href->{TABLE_NAME} or $log->logcroak('no $table_name sent to create_table()!');
    my $dbh          = $param_href->{DBH}        or $log->logcroak('no $dbh sent to create_table()!');
    my $create_query = $param_href->{QUERY}      or $log->logcroak('no $create_query sent to create_table()!');

	#create table in database specified in connection
    my $drop_query = sprintf( qq{
    DROP TABLE IF EXISTS %s
    }, $dbh->quote_identifier($table_name) );
    eval { $dbh->do($drop_query) };
    $log->error("Action: dropping $table_name failed: $@") if $@;
    $log->info("Action: $table_name dropped successfully!") unless $@;

    eval { $dbh->do($create_query) };
    $log->error( "Action: creating $table_name failed: $@" ) if $@;
    $log->info( "Action: $table_name created successfully!" ) unless $@;

    return;
}


### INTERFACE SUB ###
# Usage      : ensembl_ftp( $param_href );
# Purpose    : downloads Ensembl metazoa, fungi, plants, protists and bacteria divisions (all genomes except vertebrates)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it downloads all genomes into same dir
# See Also   : 
sub ensembl_ftp {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('ensembl_ftp() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensemblgenomes.org';

    #get new handle
    my $dbh = dbi_connect($param_href);

    #file for statistics and header information (print to outside $OUT)
    my $stat_file = path($OUT)->parent;
    $stat_file = path( $stat_file, 'statistics_ensembl_all.txt' )->canonpath;
    if ( -f $stat_file ) {
        unlink $stat_file and $log->warn("Action: unlinked $stat_file");
    }
    open my $stat_fh, '>>', $stat_file or die "can't open file: $!";

    #write header to stats file
    print {$stat_fh} "remote_path\tlocal_path\tgzip_file\theader\tNum_genes\n";

	#create INFO table in database (to import later for each division)
	my $table_info = "species_ensembl_divisions$$";
	my $create_info = sprintf( qq{
    CREATE TABLE %s (
	species_name VARCHAR(200) NOT NULL,
	species VARCHAR(200) NOT NULL,
	division VARCHAR(50) NOT NULL,
	ti INT UNSIGNED NOT NULL,
	assembly VARCHAR(100) NULL,
	assembly_accession VARCHAR(50) NULL,
	genebuild VARCHAR(50) NULL,
	variation CHAR(1) NOT NULL,
	pan_compara CHAR(1) NOT NULL,
	peptide_compara CHAR(1) NOT NULL,
	genome_alignments CHAR(1) NOT NULL,
	other_alignments CHAR(1) NOT NULL,
	core_db VARCHAR(50) NOT NULL,
	species_id INT UNSIGNED NOT NULL,
	invis VARCHAR(10),
    PRIMARY KEY(ti, species),
	KEY(species)
    )ENGINE=$ENGINE CHARSET=ascii }, $dbh->quote_identifier($table_info) );
	create_table( { TABLE_NAME => $table_info, DBH => $dbh, QUERY => $create_info, %{$param_href} } );

	#iterate over genome divisions:
	my @divisions = qw/metazoa fungi protists plants bacteria/;
	foreach my $division (@divisions) {

		#FIRST:connect to ftp to download info about genomes
		my $ftp = Net::FTP->new($REMOTE_HOST, Debug => 0) or $log->logdie( "Action: Can't connect to $REMOTE_HOST: $@" );
    	$ftp->login("anonymous",'msestak@irb.hr')         or $log->logdie( "Action: Can't login ", $ftp->message );
		$ftp->binary()                                    or $log->logdie( "Opening binary mode data connection failed for $_: $@" );
		my $remote_path = path('pub', $division, 'current');
    	$ftp->cwd($remote_path)                           or $log->logdie( "Can't change working directory ", $ftp->message );

		#get info file
		INFO:
		foreach my $info_file ($ftp->ls) {
			my $info_name = 'species_Ensembl' . ucfirst($division) . '.txt';
			#say $info_file;
			next INFO unless $info_file eq $info_name;
			if ($info_file eq $info_name) {
				$log->trace( "Action: working on $division found $info_file" );
			}

			#download and import to db
			#delete info file if it exists
			my $info_local = path(path($OUT)->parent, $info_file);
			if (-f $info_local) {
				unlink $info_local and $log->warn( "Action: unlinked $info_local" );
			}
		
			#opens a filehandle to $OUT dir and downloads file there
			open my $local_info_fh, ">>", $info_local or $log->logdie( "Can't write to $info_local:$!" );
			$ftp->get($info_file, $local_info_fh) and $log->info( "Action: download to $info_local" );
			#print $local_info_fh "\n";
			close $local_info_fh;

			#INSERT info files one after another
    		my $load_info = qq{
    		LOAD DATA INFILE '$info_local'
			INTO TABLE $table_info
			IGNORE 1 LINES
    		};
    		eval { $dbh->do($load_info, { async => 1 } ) };
			my $rows_info = $dbh->mysql_async_result;
    		$log->debug( "Action: $table_info loaded with $rows_info rows!" ) unless $@;
    		$log->debug( "Action: loading $table_info failed: $@" ) if $@;

			#DELETE genomes with same tax_ids
			my $delete_dup = qq{
			DELETE ens FROM $table_info AS ens
			WHERE ti IN (SELECT ti 
						 FROM (SELECT ti, COUNT(ti) as cnt 
			             FROM $table_info
						 GROUP BY ti
						 HAVING cnt > 1) AS x)
    		};
    		eval { $dbh->do($delete_dup, { async => 1 } ) };
			my $rows_dup = $dbh->mysql_async_result;
    		$log->debug( "Action: $table_info deleted with $rows_dup rows!" ) unless $@;
    		$log->debug( "Action: delete $table_info failed: $@" ) if $@;


		}
		$ftp->quit;   #restart for every division
	}

	#SECOND
	#iterate over genome divisions for genome download:
	my @division = qw/protists fungi metazoa plants bacteria/;
	DIVISION:
	foreach my $division (@division) {
		$log->error( qq|Report: working on $division division| );   #just for show red
		sleep 1;

		#connect to ftp to download genomes
		my $ftp;
		#$ftp->quit and $log->error( "Action: closing ftp connection for $division" );   #restart for every division
		$ftp = Net::FTP::AutoReconnect->new($REMOTE_HOST, Debug => 0) or $log->logdie( "Action: Can't connect to $REMOTE_HOST: $@" );
    	$ftp->login("anonymous",'msestak@irb.hr')         or $log->logdie( "Action: Can't login ", $ftp->message );
		$ftp->binary()                                    or $log->logdie( "Opening binary mode data connection failed for $_: $@" );
		my $remote_path = path('pub', $division, 'current', 'fasta');
    	$ftp->cwd($remote_path)                           or $log->logdie( "Can't change working directory ", $ftp->message );
		$ftp->pasv()                                   or $log->logdie( "Opening passive mode data connection failed for $_: $@" );
		$log->trace("Report: location: ", $ftp->pwd() );

		my @species_listing = $ftp->ls;
		DIR:
		foreach my $species_dir_out (@species_listing) {
			#$log->trace("Action: working with $species_dir_out" );
			#$log->trace("Report: location: ", $ftp->pwd() );
			if ($species_dir_out eq 'ancestral_alleles') {
				$log->trace( "Action: ancestral_alleles skipped" );
				next DIR;
			}

			#for testing (smaller dataset)
			#if ($species_dir_out =~ m/\A[a-e].+\z/) {
			#	next DIR and $log->trace( "Action: $species_dir_out skipped" );
			#}

			if ($species_dir_out =~ m/collection/g) {
				$ftp->cwd($species_dir_out) and $log->warn( qq|Action: cwd to $species_dir_out: working inside collection| );
				my @collection_listing = $ftp->ls;
				foreach my $species_in_coll (@collection_listing) {
					#crucial to send $ftp to the sub (else it uses old one from previous division)
            		ftp_get_proteome(
            		    { DIR => $species_in_coll, FTP => $ftp, TABLE => $table_info, DBH => $dbh, %{$param_href} } );
				}
				$ftp->cdup() and $log->warn( qq|Action: cwd out of collection: $species_dir_out| );
			}
			else {
				#normal ftp (outside collection)
				#crucial to send $ftp to the sub (else it uses old one from previous division)
            	ftp_get_proteome(
            	    { DIR => $species_dir_out, FTP => $ftp, TABLE => $table_info, DBH => $dbh, %{$param_href} } );
			}
			
		}   #foreach DIR inside division end
			$log->error( "Action: closing ftp connection for $division" );   #restart for every division

		$ftp->quit and next DIVISION;
	}   #division end

	close $stat_fh;   #collects all divisions
	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : ftp_robust($param_href );
# Purpose    : ftp download of NCBI files (generic) or some other ftp server
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it tries 10 times every 10 seconds
#            : ftp.ncbi.nih.gov is default server
# See Also   : it uses https://metacpan.org/pod/Net::FTP::Robust 
sub ftp_robust {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'ftp_download() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $OUT         = $param_href->{OUT}         or $log->logcroak( 'no $OUT specified on command line!' );
	my $REMOTE_DIR  = $param_href->{REMOTE_DIR}  or $log->logcroak( 'no $REMOTE_DIR specified on command line!' );
	my $REMOTE_FILE = $param_href->{REMOTE_FILE} or $log->logcroak( 'no $REMOTE_FILE specified on command line!' );
	my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ncbi.nih.gov';

	#params for ftp server (e.g., NCBI BLAST DB)
	my $remote_file = path($REMOTE_DIR, $REMOTE_FILE);
	#say $remote_file;
	my $local_dir  = path($OUT)->canonpath;
	#say $local_dir;

    my $ftp = Net::FTP::Robust->new
	  ( Host           => $REMOTE_HOST,
		login_attempts => 10,
		login_delay    => 10,
		user           => 'anonymous',
		password       => 'msestak@irb.hr',

      );
    
	#it needs remote FILE location, and local DIR location (not file)
	#local filename is remote filename
    $ftp->get($remote_file, $local_dir);
	
	return;
}

### INTERFACE SUB ###
# Usage      : extract_and_load_nr($param_href );
# Purpose    : extracts NCBI nr.gz file and LOADs it into MySQL database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it works on Linux only because it uses Linux named pipes
#            : it runs fork: Perl is child, MySQL parent
#            : can work with different MySQL storage engines
# See Also   : it uses https://metacpan.org/pod/PerlIO::gzip to open gziped file without decompressing it
sub extract_and_load_nr {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'extract_and_load_nr() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $OUT         = $param_href->{OUT}         or $log->logcroak( 'no $OUT specified on command line!' );
	my $INFILE      = $param_href->{INFILE}      or $log->logcroak( 'no $INFILE specified on command line!' );
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';

	#open gziped file without decompressing it
	#get date for nr file naming
    my $now  = DateTime::Tiny->now;
    my $date = $now->year . '_' . $now->month . '_' . $now->day;
	open my $nr_fh, "<:gzip", $INFILE or $log->logdie( "Can't open gzipped file $INFILE: $!" );
	
	#delete pipe if exists (you can't load into more than 1 same engine db at the same time)
	my $load_file = path($OUT, "nr_${date}_$ENGINE");   #file for LOAD DATA INFILE
	if (-p $load_file) {
		unlink $load_file and $log->trace( "Action: named pipe $load_file removed!" );
	}
	#make named pipe
	mkfifo( $load_file, 0666 ) or $log->logdie( "mkfifo $load_file failed: $!" );

	#start 2 processes (one for Perl-child and MySQL-parent)
    my $pid = fork;

	if (!defined $pid) {
		$log->logdie( "Cannot fork: $!" );
	}

	elsif ($pid == 0) {
		# Child-client process
		$log->warn( "Perl-child-client starting..." );

		#open named pipe for writing (gziped file --> named pipe)
		open my $nr_wr_fh, "+<:encoding(ASCII)", $load_file or die $!;   #+< mode=read and write
		
		#define new block for reading blocks of fasta
		{
			local $/ = ">gi";  #look in larger chunks between >gi (solo > found in header so can't use)
			local $.;          #gzip count
			my $out_cnt = 0;   #named pipe count
			#print to named pipe
			PIPE:
			while (<$nr_fh>) {
				chomp;
				#print $nr_wr_fh "$_";
				#say '{', $_, '}';
				next PIPE if $_ eq '';   #first iteration is empty?
				
				#each fasta can be multispecies with multiple gi
				#first get entire header + fasta
				my ($header_long, $fasta) = $_ =~ m{\A([^\n].+?)\n(.+)\z}sx;
				#remove illegal chars from fasta and upercase it
			    $fasta =~ s/\R//g;  #delete multiple newlines (all vertical and horizontal space)
				$fasta =~ tr/[+*-._]//;
				$fasta =~ s/\d+//;
				$fasta = uc $fasta;
				$header_long =~ s/\|\|/\|/g;
				$header_long = 'gi' . $header_long;   #gi removed as record separator (return it back)

				#split on Ctrl-A
				my @headers = split("\cA", $header_long);
				#say '{', join("}\n{", @headers), '}';
				
				#print redundant copies of fasta for each unique gi
				foreach my $header (@headers) {
					my ($gi) = $header =~ m{gi\|(\d+)\|}x;    

					#say 'GI:{', $gi, '}';

					print {$nr_wr_fh} "$gi\t$fasta\n";
					$out_cnt++;
				}

				#progress tracker
				if ($. % 1000000 == 0) {
					$log->trace( "$. lines processed!" );
				}
			}
			my $nr_file_line_cnt = $. - 1;   #first line read empty (don't know why)
			$log->warn( "File $INFILE has $nr_file_line_cnt lines!" );
			$log->warn( "File $load_file written with $out_cnt lines!" );
		}   #END block writing to pipe

		$log->warn( "Perl-child-client terminating :)" );
		exit 0;
	}
	else {
		# MySQL-parent process
		$log->warn( "MySQL-parent process, waiting for child..." );
		
		#SECOND PART:Loading file into db
		my $DATABASE = $param_href->{DATABASE}    or $log->logcroak( 'no $DATABASE specified on command line!' );
		
		#get new handle
    	my $dbh = dbi_connect($param_href);

		my $table = path($load_file)->basename;

    	#report what are you doing
    	$log->info( "---------->Importing NCBI $table" );
    	my $create_query = sprintf( qq{
    	CREATE TABLE %s (
    	gi INT UNSIGNED NOT NULL,
    	fasta MEDIUMTEXT NOT NULL,
    	PRIMARY KEY(gi)
    	)ENGINE=$ENGINE CHARSET=ascii }, $dbh->quote_identifier($table) );
		say $create_query;
		create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

		#import table
    	my $load_query = qq{
    	LOAD DATA INFILE '$load_file'
    	INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    	LINES TERMINATED BY '\n'
    	};
    	eval { $dbh->do( $load_query, { async => 1 } ) };

    	#check status while running LOAD DATA INFILE
    	{    
    	    my $dbh_check         = dbi_connect($param_href);
    	    until ( $dbh->mysql_async_ready ) {
				my $processlist_query = qq{
					SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
					WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
					};
    	        my $sth = $dbh_check->prepare($processlist_query);
    	        $sth->execute($DATABASE);
    	        my ( $time, $state );
    	        $sth->bind_columns( \( $time, $state ) );
    	        while ( $sth->fetchrow_arrayref ) {
    	            my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
    	            $log->trace( $process );
    	            sleep 10;
    	        }
    	    }
    	}    #end check LOAD DATA INFILE
    	my $rows = $dbh->mysql_async_result;
    	$log->trace( "Report: import inserted $rows rows!" );

    	#report success or failure
    	$log->error( "Report: loading $table failed: $@" ) if $@;
    	$log->debug( "Report: table $table loaded successfully!" ) unless $@;

		$dbh->disconnect;

		#communicate with child process
		waitpid $pid, 0;
	}
	$log->warn( "MySQL-parent process after child has finished" );
	unlink $load_file and $log->trace( "Named pipe $load_file removed!" );

	return;
}

### INTERFACE SUB ###
# Usage      : extract_and_load_gi_taxid( $param_href );
# Purpose    : extracts NCBI gi_taxid_prot.dmp.gz file and LOADs it into MySQL database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it works on Linux only because it uses Linux named pipes
#            : it runs fork: Perl is child, MySQL parent
# See Also   : it uses https://metacpan.org/pod/PerlIO::gzip to open gziped file without decompressing it
sub extract_and_load_gi_taxid {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'extract_and_load_gi_taxid() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $OUT         = $param_href->{OUT}         or $log->logcroak( 'no $OUT specified on command line!' );
	my $INFILE      = $param_href->{INFILE}      or $log->logcroak( 'no $INFILE specified on command line!' );
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';

	#open gziped file without decompressing it
	open my $nr_fh, "<:gzip", $INFILE or $log->logdie( "Can't open gzipped file $INFILE: $!" );

	my $load_file = path($OUT, path($INFILE)->basename . "_$ENGINE");   #file for LOAD DATA INFILE
	$load_file =~ s/\.dmp\.gz//g;
	$load_file =~ s/\./_/g;
	#make named pipe
	mkfifo( $load_file, 0666 ) or $log->logdie( "mkfifo $load_file failed: $!" );

	#start 2 processes (one for Perl-child and second for Mysql-parent)
    my $pid = fork;

	if (!defined $pid) {
		$log->logdie( "Cannot fork: $!" );
	}

	elsif ($pid == 0) {
		# Child-client process
		$log->warn( "Perl-child-client starting..." );

		#open named pipe for writing (gziped file --> named pipe)
		open my $nr_wr_fh, "+<:encoding(ASCII)", $load_file or $log->logdie( "Can't open named pipe $load_file for Perl writing:$!" );   #+< mode=read and write
		
		#define new block for reading blocks of fasta
		{
			local $.;          #gzip count
			my $out_cnt = 0;   #named pipe count
			#print to named pipe
			PIPE:
			while (<$nr_fh>) {
				chomp;
				next PIPE if $_ eq '';   #first iteration is empty?
				
				print $nr_wr_fh "$_\n";
				$out_cnt++;
				
				#progress tracker
				if ($. % 1000000 == 0) {
					$log->trace( "$. lines processed!" );
				}
			}
			my $input_file_line_cnt = $.;
			$log->debug( "File $INFILE has $input_file_line_cnt lines!" );
			$log->debug( "File $load_file written with $out_cnt lines!" );
		}   #END block writing to pipe

		#keep child process alive for short files
        sleep 10;
		$log->warn( "Perl-child-client terminating :)" );
		exit 0;
	}
	else {
		# MySQL-parent process
		$log->warn( "MySQL-parent process, waiting for child..." );
		
		#SECOND PART:Loading file into db
		my $DATABASE = $param_href->{DATABASE}    or $log->logcroak( 'no $DATABASE specified on command line!' );
		
		#get new handle
    	my $dbh = dbi_connect($param_href);

		my $table = path($load_file)->basename;

    	#report what are you doing
    	$log->info( "---------->Importing NCBI $table" );

    	#create table
    	my $create_query = sprintf( qq{
    	CREATE TABLE $table (
    	gi INT UNSIGNED NOT NULL,
    	ti INT UNSIGNED NOT NULL,
    	PRIMARY KEY(gi),
		KEY(ti)
    	)ENGINE=$ENGINE CHARSET=ascii }, $dbh->quote_identifier($table) );
		create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );
		say $create_query;

		#import table
    	my $load_query = qq{
    	LOAD DATA INFILE '$load_file'
    	INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    	LINES TERMINATED BY '\n'
    	};
    	eval { $dbh->do( $load_query, { async => 1 } ) };

    	#check status while running LOAD DATA INFILE
    	{    
    	    my $dbh_check         = dbi_connect($param_href);
    	    until ( $dbh->mysql_async_ready ) {
				my $processlist_query = qq{
					SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
					WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
					};
    	        my $sth = $dbh_check->prepare($processlist_query);
    	        $sth->execute($DATABASE);
    	        my ( $time, $state );
    	        $sth->bind_columns( \( $time, $state ) );
    	        while ( $sth->fetchrow_arrayref ) {
    	            my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
    	            $log->trace( $process );
    	            sleep 10;
    	        }
    	    }
    	}    #end check LOAD DATA INFILE
    	my $rows = $dbh->mysql_async_result;
    	$log->trace( "Report: import inserted $rows rows!" );

    	#report success or failure
    	$log->error( "Report: loading $table failed: $@" ) if $@;
    	$log->debug( "Report: table $table loaded successfully!" ) unless $@;

		$dbh->disconnect;

		#communicate with child process
		waitpid $pid, 0;
	}
	$log->warn( "MySQL-parent process after child has finished" );
	unlink $load_file and $log->trace( "Named pipe $load_file removed!" );

	return;
}


### INTERFACE SUB ###
# Usage      : ti_gi_fasta( $param_href );
# Purpose    : JOINs gi/fasta from nr database and gi/taxid from gi_taxid_prot.dmp
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : 
# See Also   : 
sub ti_gi_fasta {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'ti_gi_fasta() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE}    or $log->logcroak( 'no $DATABASE specified on command line!' );
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#first prompt to select nr table
    my $select_tables = qq{
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '$DATABASE'
    };
    my @tables = map { $_->[0] } @{ $dbh->selectall_arrayref($select_tables) };

    #ask to choose nr
    my $table_nr= prompt 'Choose NR table to use ',
      -menu => [ @tables ],
	  -number,
      '>';
    $log->trace( "Report: using NR: $table_nr" );

    #ask to choose gi_taxid
    my $table_gi_taxid= prompt 'Choose GI_TAXID_PROT table to use ',
      -menu => [ @tables ],
	  -number,
      '>';
    $log->trace( "Report: using GI_TAXID_PROT: $table_gi_taxid" );

    #report what are you doing
    $log->info( "---------->JOIN-ing two tables: $table_nr and $table_gi_taxid" );

    #drop table that is product of JOIN
	my $table_base = "nr_ti_gi_fasta_$ENGINE";
    my $create_query = sprintf( qq{
    CREATE TABLE %s (
    ti INT UNSIGNED NOT NULL,
    gi INT UNSIGNED NOT NULL,
	fasta MEDIUMTEXT NOT NULL,
    PRIMARY KEY(ti, gi)
    )ENGINE=$ENGINE CHARSET=ascii }, $dbh->quote_identifier($table_base) );
	create_table( { TABLE_NAME => $table_base, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

    my $insert_query = qq{
    INSERT INTO $table_base (ti, gi, fasta)
    SELECT gt.ti, nr.gi, nr.fasta
    FROM $table_nr AS nr
    INNER JOIN $table_gi_taxid AS gt ON nr.gi = gt.gi
    };
    eval { $dbh->do($insert_query, { async => 1 } ) };

    #check status while running
    {    
        my $dbh_check         = dbi_connect($param_href);
        until ( $dbh->mysql_async_ready ) {
			my $processlist_query = qq{
				SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
				WHERE DB = ? AND INFO LIKE 'INSERT%';
				};
            my $sth = $dbh_check->prepare($processlist_query);
            $sth->execute($DATABASE);
            my ( $time, $state );
            $sth->bind_columns( \( $time, $state ) );
            while ( $sth->fetchrow_arrayref ) {
                my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
                $log->trace( $process );
                sleep 10;
            }
        }
    }    #end check
    my $rows = $dbh->mysql_async_result;
    $log->trace( "Report: import inserted $rows rows!" );

    #report success or failure
    $log->error( "Report: loading $table_base failed: $@" ) if $@;
    $log->debug( "Report: table $table_base loaded successfully!" ) unless $@;

	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : import_names( $param_href );
# Purpose    : loads names.tsv.updated to MySQL database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : works on new format with tabs
# See Also   :
sub import_names {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_names() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $INFILE   = $param_href->{INFILE}   or $log->logcroak('no $INFILE specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $table = path($INFILE)->basename;
    $table =~ s/\./_/g;    #for files that have dots in name)

    #get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info("---------->Importing names $table");

    my $create_query = sprintf( qq{
    CREATE TABLE %s (
    id INT UNSIGNED AUTO_INCREMENT NOT NULL,
    ti INT UNSIGNED NOT NULL,
    species_name VARCHAR(200) NOT NULL,
    species_synonym VARCHAR(100),
    name_type VARCHAR(100),
    PRIMARY KEY(id, ti),
	KEY(ti),
    KEY(species_name)
    )ENGINE=$ENGINE CHARACTER SET=ascii }, $dbh->quote_identifier($table) );
	create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );
	say $create_query;

    #import table
    my $load_query = qq{
    LOAD DATA INFILE '$INFILE'
    INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n' 
    (ti, species_name, species_synonym, name_type) 
    };
    eval { $dbh->do( $load_query, { async => 1 } ) };

    #check status while running
    my $dbh_check             = dbi_connect($param_href);
    until ( $dbh->mysql_async_ready ) {
        my $processlist_query = qq{
        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
        };
        my ( $time, $state );
        my $sth = $dbh_check->prepare($processlist_query);
        $sth->execute($DATABASE);
        $sth->bind_columns( \( $time, $state ) );
        while ( $sth->fetchrow_arrayref ) {
            my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
            $log->trace($process);
            sleep 1;
        }
    }
    my $rows = $dbh->mysql_async_result;
    $log->trace( "Report: import inserted $rows rows!" );

    #report success or failure
    $log->error( "Report: loading $table failed: $@" ) if $@;
    $log->debug( "Report: table $table loaded successfully!" ) unless $@;

    return;
}

### INTERFACE SUB ###
# Usage      : import_nodes( $param_href );
# Purpose    : loads nodes.tsv.updated to MySQL
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : new format with tabs
# See Also   :
sub import_nodes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak ('import_nodes() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

	my $INFILE   = $param_href->{INFILE}      or $log->logcroak( 'no $INFILE specified on command line!' );
	my $DATABASE = $param_href->{DATABASE}    or $log->logcroak( 'no $DATABASE specified on command line!' );
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $table    = path($INFILE)->basename;
    $table =~ s/\./_/g;    #for files that have dots in name)

    #get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info( "---------->Importing nodes $table" );
    my $create_query = sprintf( qq{
    CREATE TABLE IF NOT EXISTS %s (
    ti INT UNSIGNED NOT NULL,
    parent_ti INT UNSIGNED NOT NULL,
    PRIMARY KEY(ti),
    KEY(parent_ti)
    )ENGINE=$ENGINE CHARACTER SET=ascii }, $dbh->quote_identifier($table) );
	create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

    #import table
    my $load_query = qq{
    LOAD DATA INFILE '$INFILE'
    INTO TABLE $table } . q{ FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n' 
    };
    eval { $dbh->do( $load_query, { async => 1 } ) };

    #check status while running
    my $dbh_check             = dbi_connect($param_href);
    until ( $dbh->mysql_async_ready ) {
        my $processlist_query = qq{
        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE DB = ? AND INFO LIKE 'LOAD DATA INFILE%';
        };
        my ( $time, $state );
        my $sth = $dbh_check->prepare($processlist_query);
        $sth->execute($DATABASE);
        $sth->bind_columns( \( $time, $state ) );
        while ( $sth->fetchrow_arrayref ) {
            my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
            $log->trace( $process );
            sleep 1;
        }
    }
    my $rows = $dbh->mysql_async_result;
    $log->trace( "Report: import inserted $rows rows!" );

    #report success or failure
    $log->error( "Report: loading $table failed: $@" ) if $@;
    $log->debug( "Report: table $table loaded successfully!" ) unless $@;

    return;
}








1;
__END__

=encoding utf-8

=head1 NAME

CollectGenomes - Downloads genomes from Ensembl FTP (and NCBI nr db) and builds BLAST database (this is modulino - call it directly).

=head1 SYNOPSIS

 Part I -> download genomes from Ensembl:

 perl ./lib/CollectGenomes.pm --mode=create_db -ho localhost -d nr -p msandbox -u msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./lib/CollectGenomes.pm --mode=ensembl_ftp --out=./ensembl_ftp/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./lib/CollectGenomes.pm --mode=ensembl_vertebrates --out=./ensembl_vertebrates/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 Part II -> download genomes from NCBI:

 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /blast/db/FASTA/ -rf nr.gz

 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf gi_taxid_prot.dmp.gz

 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf taxdump.tar.gz

 Part III -> load nr into database

 perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if ./nr/nr_10k.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=gi_taxid -if ./nr/gi_taxid_prot.dmp.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr -ho localhost -u msandbox -p msandbox --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 Part IV -> set phylogeny for focal species:

 perl ./lib/CollectGenomes.pm --mode=import_names -if ./nr/names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=import_nodes -if ./nr/nodes_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB




 perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 or
 perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl blastdb_analysis.pl -mode=call_phylo -no nodes_martin7 -t 2759 -org eu --proc=proc_create_phylo16278 -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 Part V -> get genomes from nr base:

 perl ./bin/CollectGenomes.pm --mode=get_existing_ti --in=./t_eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=get_missing_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=delete_extra_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=delete_full_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=print_nr_genomes --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=copy_existing_genomes --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya_old/  --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


 Part VI -> prepare and run cd-hit
 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=./data_in/t_eukarya/ --out=./data_out/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_29_07_15/data/archaea/ --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


 perl ./bin/CollectGenomes.pm --mode=run_cdhit --in=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock -v

 Part VII -> prepare BLAST and run it:

=head1 DESCRIPTION

CollectGenomes is modulino that downloads genomes (actually proteomes) from Ensembl FTP servers. It names them by tax_id.
It can also download NCBI nr database and extract genomes from it (requires MySQL).
It runs clustering with cd-hit and builds a BLAST database per species analyzed.

To use different functionality use specific modes.
Possible modes:

 create_db                     => \&create_database,
 ftp                           => \&ftp_robust,
 extract_nr                    => \&extract_nr,
 load_nr                       => \&load_nr,
 extract_and_load_nr           => \&extract_and_load_nr,
 gi_taxid                      => \&extract_and_load_gi_taxid,
 ti_gi_fasta                   => \&ti_gi_fasta,
 get_existing_ti               => \&get_existing_ti,
 import_names                  => \&import_names,
 import_nodes                  => \&import_nodes,
 get_missing_genomes           => \&get_missing_genomes,
 delete_extra_genomes          => \&delete_extra_genomes,
 delete_full_genomes           => \&delete_full_genomes,
 print_nr_genomes              => \&print_nr_genomes,
 copy_existing_genomes         => \&copy_existing_genomes,
 ensembl_vertebrates           => \&ensembl_vertebrates,
 ensembl_ftp                   => \&ensembl_ftp,
 prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
 run_cdhit                     => \&run_cdhit,

For help write:

 perl CollectGenomes.pm -h
 perl CollectGenomes.pm -m


=head1 LICENSE

Copyright (C) MOCNII Martin Sebastijan Šestak

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

mocnii E<lt>msestak@irb.hrE<gt>

=cut

