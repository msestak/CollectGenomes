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
use XML::Twig;

our $VERSION = "0.01";

our @EXPORT_OK = qw{
	main
    init_logging
	get_parameters_from_cmd
	dbi_connect
	create_database
	create_table
	capture_output
	ftp_robust
	extract_and_load_nr
    extract_and_load_gi_taxid
	del_virus_from_nr
	ti_gi_fasta
	get_existing_ti
	import_names
	import_nodes
	run_mysqldump
	fn_create_tree
	fn_retrieve_phylogeny
	prompt_fn_retrieve
	proc_create_phylo
	call_proc_phylo
	jgi_download
	nr_genome_counts
	export_all_nr_genomes
    get_missing_genomes
	del_nr_genomes
	del_total_genomes
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
    my $VERBOSE  = $param_href->{VERBOSE};
	
    #get dump of param_href if -v (VERBOSE) flag is on (for debugging)
    my $dump_print = sprintf( Dumper($param_href) ) if $VERBOSE;
    $log->debug( '$param_href = ', "$dump_print" ) if $VERBOSE;


    #need to create dispatch table for different usage depending on mode requested
    #dispatch table is hash (could be also hash_ref)
    my %dispatch = (
        create_db                     => \&create_database,
        nr_ftp                        => \&ftp_robust,
        extract_nr                    => \&extract_nr,
        load_nr                       => \&load_nr,
        extract_and_load_nr           => \&extract_and_load_nr,
        gi_taxid                      => \&extract_and_load_gi_taxid,
		del_virus_from_nr             => \&del_virus_from_nr,
        import_names                  => \&import_names,
        import_nodes                  => \&import_nodes,
        ti_gi_fasta                   => \&ti_gi_fasta,
        mysqldump                     => \&run_mysqldump,
        fn_tree                       => \&fn_create_tree,
        fn_retrieve                   => \&fn_retrieve_phylogeny,
        prompt_ph                     => \&prompt_fn_retrieve,
        proc_phylo                    => \&proc_create_phylo,
        call_phylo                    => \&call_proc_phylo,
		jgi_download                  => \&jgi_download,
        get_existing_ti               => \&get_existing_ti,
        nr_genome_counts              => \&nr_genome_counts,
		export_all_nr_genomes         => \&export_all_nr_genomes,
        get_missing_genomes           => \&get_missing_genomes,
        del_nr_genomes                => \&del_nr_genomes,
        del_total_genomes             => \&del_total_genomes,
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
		$NODES, $NAMES,    %TABLES,   $ORG,      $TAX_ID, $MAP,
		$OUT,   $IN,       $OUTFILE, $INFILE,
        $HOST,  $DATABASE, $USER,    $PASSWORD, $PORT,   $SOCKET, $CHARSET, $ENGINE,
		$REMOTE_HOST,      $REMOTE_DIR,         $REMOTE_FILE,
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
        'tables|tbl=s'     => \%TABLES,        #accepts 1 or more arguments
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
            TABLES      => \%TABLES,
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
	if (-p $load_file) { unlink $load_file and $log->warn(qq|Action: named pipe $load_file deleted|); }
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
	$log->trace("Report: $create_query");

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
    $log->info( "Report: import inserted $rows rows!" );

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


### INTERFACE SUB ###
# Usage      : fn_create_tree( $param_href );
# Purpose    : installs function fn_create_tree in database (and tree table)
# Returns    : nothing
# Parameters : ( $param_href)
# Throws     : croaks for parameters
# Comments   : first part in chain, can be ignored once installed
# See Also   :
sub fn_create_tree {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'fn_create_tree() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $dbh   = dbi_connect($param_href);
    my $NODES = $param_href->{NODES} or $log->logcroak( 'no $NODES file specified on command line!' );
    my $TAX_ID = $param_href->{TAX_ID} or $log->logcroak( 'no $TAX_ID specified on command line!' );

    #Using TAX_ID to get unique table names
    my $table    = "tree$TAX_ID";
    my $function = "fn_create_$table";

    my $create_table_query = sprintf( qq{
    CREATE TABLE IF NOT EXISTS %s (
    ti INT UNSIGNED NOT NULL,
    tax_level TINYINT UNSIGNED NOT NULL,
    INDEX USING HASH (ti)
    )ENGINE=MEMORY }, $dbh->quote_identifier($table) );
	create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_table_query, %{$param_href} } );
	$log->trace("Report: $create_table_query");

    #drop unique fn_create_tree
    my $drop_fn_query = qq{
    DROP FUNCTION IF EXISTS $function
    };
    eval { $dbh->do($drop_fn_query) };
    $log->error( "Action: dropping $function failed: $@" ) if $@;
    $log->debug( "Action: function $function dropped successfully!" ) unless $@;

    #create unique fn_create_tree function
    #use $NODES from command line
    my $create_fn_query = qq{
    CREATE FUNCTION $function (var_ti INT) RETURNS INT
        DETERMINISTIC
        MODIFIES SQL DATA
        
    BEGIN
        
        DECLARE tax_level INT;
        SET tax_level = 1;
        -- insert the top level ti from outside variable(var_ti)
        DELETE $table FROM $table;
        INSERT INTO $table
        SELECT ti, tax_level
            FROM $NODES AS no
            WHERE no.ti = var_ti;
        -- Loop through sub-levels
        WHILE ROW_COUNT() > 0
            DO
            SET tax_level = tax_level + 1;
            -- insert the taxonomy levels under the parent
            INSERT INTO $table
            SELECT phylogeny.ti, tax_level
                FROM $NODES AS family_node 
                JOIN $NODES AS phylogeny ON family_node.ti = phylogeny.parent_ti
                JOIN $table ON $table.ti = family_node.ti
                WHERE $table.tax_level = tax_level - 1;
        
        END WHILE; 
        
        RETURN var_ti;
         
    END
    };
    eval { $dbh->do($create_fn_query) };
    $log->error( "Action: creating $function failed: $@" ) if $@;
    $log->debug( "Action: function $function created successfully!" ) unless $@;

    $dbh->disconnect;

    return;
}

### INTERFACE SUB ###
# Usage      : fn_retrieve_phylogeny( $param_href );
# Purpose    : installs function fn_retrieve_phylogeny in database
# Returns    : nothing
# Parameters : ( $param_href)
# Throws     : croaks for parameters
# Comments   : second function in chain, can be ignored once installed
# See Also   :
sub fn_retrieve_phylogeny {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('fn_retrieve_phylogeny() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $dbh    = dbi_connect($param_href);
    my $NODES  = $param_href->{NODES} or $log->logcroak('no $NODES file specified on command line!');
    my $TAX_ID = $param_href->{TAX_ID} or $log->logcroak('no $TAX_ID file specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    (my $names = $NODES) =~ s/\A(.)(?:.+?)_(.+)\z/$1ames_$2/;

    #needed only once at start of entire procedure
    $dbh->{AutoCommit} = 0;    # enable transactions, if possible
    eval {
        my $update_query = qq{
        UPDATE $NODES
        SET parent_ti = 100000000
        WHERE ti = 1 AND parent_ti = 1
        };
        $dbh->do($update_query);
        $dbh->commit;          # commit the changes if we get this far
        $log->debug("Table $NODES updated successfully (SET parent_ti = 100000000 WHERE ti = 1 AND parent_ti = 1)");
    };
    if ($@) {
        $log->logcarp("Transaction aborted because $@");

        # now rollback to undo the incomplete changes
        # but do it in an eval{} as it may also fail
        eval { $dbh->rollback };

        # add other application on-error-clean-up code here
        if ($@) {
            $log->logcarp("Updating $NODES failed! Transaction failed to commit and failed to rollback!");
        }
    }

    #using $$ as process_id to get unique table_name
    my $table           = "tree_fn_ret_ph$TAX_ID";
    my $table_phylogeny = "retrieve_phylogeny$TAX_ID";
    my $function        = "fn_retrieve_phylogeny$TAX_ID";

    #back to AUTOCOMMIT mode, we don't need transactions anymore
    $dbh->{AutoCommit} = 1;

    #create unique tree table
    my $create_table_query = qq{
    CREATE TABLE IF NOT EXISTS $table (
    ti INT UNSIGNED NOT NULL,
    tax_level TINYINT UNSIGNED NOT NULL
    )ENGINE=$ENGINE
    };
	create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_table_query, %{$param_href} } );
	$log->trace("Report: $create_table_query");

    #create unique retrieve_phylogeny table
    my $create_table_query_phylogeny = qq{
    CREATE TABLE IF NOT EXISTS $table_phylogeny (
    species_name VARCHAR(200) NOT NULL,
    ti INT UNSIGNED NOT NULL,
    parent_ti INT UNSIGNED NOT NULL,
    tax_level TINYINT UNSIGNED NOT NULL,
	PRIMARY KEY(tax_level),
	UNIQUE KEY(ti)
    )ENGINE=$ENGINE
    };
	create_table( { TABLE_NAME => $table_phylogeny, DBH => $dbh, QUERY => $create_table_query_phylogeny, %{$param_href} } );
	$log->trace("Report: $create_table_query_phylogeny");

    #drop unique fn_retrieve_phylogeny
    my $drop_fn_query = qq{
    DROP FUNCTION IF EXISTS $function
    };
    eval { $dbh->do($drop_fn_query) };
    $log->error( "Action: dropping $function failed: $@" ) if $@;
    $log->debug( "Action: function $function dropped successfully!" ) unless $@;

    #create unique fn_retrieve_phylogeny function
    #use $NODES from command line
    my $create_fn_query = qq{
    CREATE FUNCTION $function (var_ti INT) RETURNS INT
        DETERMINISTIC
        MODIFIES SQL DATA
        
    BEGIN
        
        DECLARE tax_level INT;
        SET tax_level = 1;
        -- insert the top level
        DELETE $table FROM $table;
        INSERT INTO $table
        SELECT ti, tax_level
            FROM $NODES AS nd
            WHERE nd.ti = var_ti;
        -- Loop through sub-levels
        WHILE ROW_COUNT() > 0 
            DO
            SET tax_level = tax_level + 1;
            -- insert the taxonomy levels under the parent
            INSERT INTO $table
            SELECT nd.parent_ti, tax_level
                FROM $NODES AS nd
                JOIN $table ON $table.ti = nd.ti
                WHERE $table.tax_level = tax_level - 1;
        
        END WHILE; 
        
        DELETE $table FROM $table
        WHERE ti = 100000000;
        DELETE $table FROM $table
        WHERE ti = 1; -- deletes the ps0
        
        DELETE $table_phylogeny FROM $table_phylogeny;
        
        INSERT INTO $table_phylogeny
        SELECT GROUP_CONCAT(na.species_name), tree.ti, nd.parent_ti, tree.tax_level
            FROM $table AS tree
            INNER JOIN $NODES AS nd ON nd.ti = tree.ti
            INNER JOIN $names AS na ON na.ti = tree.ti
            GROUP BY tree.tax_level
            ORDER BY tree.tax_level DESC;

        RETURN var_ti;
         
    END
    
    
    };
    eval { $dbh->do($create_fn_query) };
    $log->debug( "Creating $function failed: $@" ) if $@;
    $log->debug( "Function $function created successfully!" ) unless $@;

    $dbh->disconnect;

    return;
}

### INTERFACE SUB ###
# Usage      : prompt_fn_retrieve( $param_href );
# Purpose    : prompt and modify retrieve_phylogeny table
# Returns    : nothing
# Parameters : ( $param_href)
# Throws     : croaks for parameters
# Comments   : testing sub
# See Also   :
sub prompt_fn_retrieve {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'prompt_fn_retrieve() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $dbh    = dbi_connect($param_href);
    my $TAX_ID = $param_href->{TAX_ID} or $log->logcroak('no $TAX_ID file specified on command line!');

    #using $$ as process_id to get unique table_name
    my $table_ret       = "tree_fn_ret_ph$TAX_ID";
    my $table_phylogeny = "retrieve_phylogeny$TAX_ID";
    my $function        = "fn_retrieve_phylogeny$TAX_ID";

    #test function on tax_id from command line
    my $test_fn_retrieve_query = qq{
    SELECT $function(?)
    };
    my $sth = $dbh->prepare($test_fn_retrieve_query);
    eval { $sth->execute($TAX_ID); };

    #uses format to extract multiple rowsets from procedures
    do {
        my ( $i, $colno );
        $log->trace( "Rowset " . ++$i . "\n---------------------------------------\n" );
        foreach $colno ( 0 .. $sth->{NUM_OF_FIELDS} - 1 ) {
            $log->trace( $sth->{NAME}->[$colno] . "\t" );
        }
        my ( $field, @row );
        while ( @row = $sth->fetchrow_array() ) {
            foreach $field ( 0 .. $#row ) {
                $log->trace( $row[$field] . "\t" );
            }
        }
    } until ( !$sth->more_results );

    $sth->finish;    #end of first check (calling function)
    $log->error( "Action: query $test_fn_retrieve_query failed: $@" ) if $@;
    $log->debug( "Action: testing $function succeeded!" ) unless $@;

    #check: looking at retrieve_phylogeny table to see if anything there
    my $test_ph_query = qq{
    SELECT species_name, ti, parent_ti, tax_level 
    FROM $table_phylogeny
    };
    my $sth2 = $dbh->prepare($test_ph_query);
    eval { $sth2->execute; };

    ### Print the header
    $log->trace( "species_name                                                                        ti        parent_ti     tax_level" );
    $log->trace( "==================================================================================  ========  ========== =========" );
    while ( my ( $species_name, $ti, $parent_ti, $tax_level ) = $sth2->fetchrow_array() ) {
        my $line = sprintf( "%-82s %9d %14d %9d\n", $species_name, $ti, $parent_ti, $tax_level );
		$log->trace( $line );
    }

    $sth2->finish;    #end of check (checking table retrieve_phylogeny)
    $log->error( "Action: query $test_ph_query failed: $@" ) if $@;
    $log->debug( "Action: testing $table_phylogeny succeeded!" ) unless $@;

    #ask to choose path (from another prompt of from list)
    my $menu
      = prompt
      'Choose how to specify tax_ids to keep (a = from prompt, b = from csv list):', "\n",
      'hs19:131567, 2759, 33154, 1452651, 33208, 6072, 33213, 33511, 7711, 1452661, 7742, 117571, 32523, 32524, 40674, 9347, 1437010, 314146, 9443', "\n",
      'dm14:131567, 2759, 33154, 1452651, 33208, 6072, 33213, 33317, 6656, 197562, 33340, 33392, 7147, 7215',
      -menu => [ 'prompt', 'list' ],
      '>';
    $log->debug( $menu );        # returns string 'list' or 'prompt'

    my $tis;                     #tax_ids to send to server
    if ( $menu eq 'prompt' ) {

        #retrieve data from prompt
        my @remaining_tis;
        my $sth_prompt = $dbh->prepare($test_ph_query);
        $sth_prompt->execute;
        $log->trace( "species_name                                                                        ti        parent_ti     tax_level" );
        $log->trace( "==================================================================================  ========  ========== ========= " );
        while ( my ( $species_name, $ti, $parent_ti, $tax_level ) = $sth_prompt->fetchrow_array() ) {
            my $line = sprintf( "%-82s %9d %14d %9d\n", $species_name, $ti, $parent_ti, $tax_level );
			$log->trace( $line );
            my $continue = prompt( "Delete? ", -yn1 );
            $log->trace( "{$continue}" );
            if ( $continue eq 'y' ) {
                $log->trace( "$tax_level to delete" );
            }
            else {
                push @remaining_tis, $ti;
            }
        }
        $tis = join( ', ', @remaining_tis );            #csv format needed for IN clause in SQL query
        $log->debug( $tis );
        $sth_prompt->finish;
    }
    else {                                              #give a list here (csv format)
        $tis = prompt('List of tis:');
        $log->debug( $tis );
    }

    #delete tax_ids that are not wanted from retrieve_phylogeny table
    my $delete_tis_query = qq{
    DELETE $table_phylogeny FROM $table_phylogeny
    WHERE ti NOT IN ($tis)
    };
    eval { $dbh->do($delete_tis_query); };
    $log->error( "Action: query $delete_tis_query failed: $@" ) if $@;
    $log->debug( "Action: table $table_phylogeny updated!" ) unless $@;

    #check: looking at retrieve_phylogeny table to see if it is updated
    my $sth_check = $dbh->prepare($test_ph_query);
    eval { $sth_check->execute; };

    ### Print the header
    $log->trace( "species_name                                                                        ti        parent_ti  tax_level" );
    $log->trace( "==================================================================================  ========  ========== =========" );
    while ( my ( $species_name, $ti, $parent_ti, $tax_level ) = $sth_check->fetchrow_array() ) {
        my $line = sprintf( "%-82s %9d %14d %9d\n", $species_name, $ti, $parent_ti, $tax_level );
		$log->trace( $line );
    }
    $sth_check->finish;    #end of check (checking table retrieve_phylogeny)

    #alter table to sort it from 1 tp ps_max
	$dbh->{AutoCommit} = 0;    # enable transactions, if possible
    eval {
        my $drop_query = qq{
        DROP TABLE IF EXISTS ${table_phylogeny}_new
        };
        $dbh->do($drop_query);
		my $create_query = qq{
		CREATE TABLE ${table_phylogeny}_new LIKE ${table_phylogeny}
		};
        $dbh->do($create_query);
		my $alter_query = qq{
		ALTER TABLE ${table_phylogeny}_new CHANGE COLUMN tax_level tax_level TINYINT NOT NULL AUTO_INCREMENT
		};
        $dbh->do($alter_query);

		my $insert_query = qq{
		INSERT INTO ${table_phylogeny}_new (species_name, ti, parent_ti)
		SELECT species_name, ti, parent_ti FROM ${table_phylogeny}
		ORDER BY tax_level DESC
		};
        $dbh->do($insert_query);
		my $drop_query2 = qq{
		DROP TABLE ${table_phylogeny}
		};
        $dbh->do($drop_query2);
		my $rename_query = qq{
		RENAME TABLE ${table_phylogeny}_new TO ${table_phylogeny}
		};
        $dbh->do($rename_query);

        $dbh->commit;          # commit the changes if we get this far
		$log->debug( "Table $table_phylogeny updated successfully!");
    };
    if ($@) {
        $log->logcarp( "Action: transaction aborted because $@" );

        # now rollback to undo the incomplete changes
        # but do it in an eval{} as it may also fail
        eval { $dbh->rollback };

        # add other application on-error-clean-up code here
        if ($@) {
            $log->logcarp( "Action: updating $table_phylogeny failed! Transaction failed to commit and failed to rollback!" );
        }
    }

	#return back to AutoCommit
	$dbh->{AutoCommit} = 1;

    #check2: looking at retrieve_phylogeny table to see if it is updated
    my $sth_check2 = $dbh->prepare($test_ph_query);
    eval { $sth_check2->execute; };

    ### Print the header
    $log->trace( "species_name                                                                        ti        parent_ti  tax_level" );
    $log->trace( "==================================================================================  ========  ========== =========" );
    while ( my ( $species_name, $ti, $parent_ti, $tax_level ) = $sth_check2->fetchrow_array() ) {
        my $line = sprintf( "%-82s %9d %14d %9d\n", $species_name, $ti, $parent_ti, $tax_level );
		$log->trace ( $line );
    }
    $sth_check2->finish;    #end of check2 (checking table retrieve_phylogeny)
    $log->error( "Action: query $test_ph_query failed: $@" ) if $@;
    $log->debug( "Action: fetching $table_phylogeny succeeded!" ) unless $@;

    $dbh->disconnect;

    return;
}

### INTERFACE SUB ###
# Usage      : proc_create_phylo( $param_href );
# Purpose    : installs function proc_create_phylo in database
# Returns    : nothing
# Parameters : ( $param_href)
# Throws     : croaks for parameters
# Comments   : need to be called together with create functions subs
#            : because it depends on process id to find functions and tables
# See Also   :
sub proc_create_phylo {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'proc_create_phylo() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $dbh    = dbi_connect($param_href);
    my $NODES  = $param_href->{NODES}  or $log->logcroak('no $NODES file specified on command line!');
    my $TAX_ID = $param_href->{TAX_ID} or $log->logcroak('no $TAX_ID file specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';

    #using process_id $$ to get unique table_name
    my $table_phylo     = "phylo_${TAX_ID}";
    my $procedure       = "proc_create_${table_phylo}";    #phylo table has $TAX_ID
    my $table_tree      = "tree$TAX_ID";
    my $table_phylogeny = "retrieve_phylogeny$TAX_ID";

    #drop unique proc_create_phylo$TAX_ID
    my $drop_proc_query = qq{
    DROP PROCEDURE IF EXISTS $procedure
    };
    eval { $dbh->do($drop_proc_query) };
    $log->error( "Action: dropping $procedure failed: $@" ) if $@;
    $log->debug( "Action: procedure $procedure dropped successfully!" ) unless $@;

    #create unique proc_create_phylo$$ procedure
    #use $NODES from command line
    my $create_proc_query = qq{
    CREATE PROCEDURE $procedure (IN ext_var_taxid INT)

    BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE var_ti INT UNSIGNED;
        DECLARE cursor1 CURSOR FOR SELECT ti FROM $table_phylogeny ORDER BY tax_level ASC;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        DROP TABLE IF EXISTS $table_phylo;
        CREATE TABLE IF NOT EXISTS $table_phylo (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        PRIMARY KEY(id)
        )ENGINE=$ENGINE DEFAULT CHARSET=ascii;

        OPEN cursor1;
        
            SET \@num = 1;

            read_loop: LOOP
                FETCH cursor1 INTO var_ti;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                IF var_ti IS NOT NULL THEN
                    SELECT fn_create_$table_tree(var_ti);

                    SET \@ti1 = CONCAT('ALTER TABLE $table_phylo ADD COLUMN ps', \@num, ' INT(11) UNSIGNED NULL');
                    PREPARE stmt FROM \@ti1;
                    EXECUTE stmt;
                    DEALLOCATE PREPARE stmt;

                    SET \@ti2 = CONCAT('INSERT INTO $table_phylo (ps', \@num, ') SELECT ti FROM $table_tree');
                    PREPARE stmt FROM \@ti2;
                    EXECUTE stmt;
                    DEALLOCATE PREPARE stmt;

                    SET \@ti3 = CONCAT('CREATE INDEX ps', \@num, '_index ON $table_phylo(ps', \@num, ')');
                    PREPARE stmt FROM \@ti3;
                    EXECUTE stmt;
                    DEALLOCATE PREPARE stmt;

                    IF \@num > 1 THEN 
                        SET \@ti4 = CONCAT('DELETE lf FROM $table_phylo AS lf INNER JOIN $table_phylo AS rh ON lf.ps', \@num - 1, ' = rh.ps', \@num);
                        PREPARE stmt FROM \@ti4;
                        EXECUTE stmt;
                        DEALLOCATE PREPARE stmt;
                    END IF;

                SET \@num = \@num + 1;

                END IF;

            END LOOP;

        CLOSE cursor1;
    END
    };
    eval { $dbh->do($create_proc_query) };
    $log->error( "Action: creating $procedure failed: $@" ) if $@;
    $log->debug( "Action: procedure $procedure created successfully!" ) unless $@;

    $dbh->disconnect;

    return;
}

### INTERFACE SUB ###
# Usage      : call_proc_phylo( $param_href );
# Purpose    : calls proc_create_phylo and creates phylo table
# Returns    : nothing
# Parameters : ( $param_href, $TAX_ID)
# Throws     : croaks for parameters
# Comments   : needs $TAX_ID from command line
# See Also   :
sub call_proc_phylo {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'call_proc_phylo() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $dbh    = dbi_connect($param_href);
    my $TAX_ID = $param_href->{TAX_ID} or $log->logcroak( 'no $TAX_ID specified on command line!' );
    my $NODES  = $param_href->{NODES}  or $log->logcroak( 'no $NODES specified on command line!' );

    #using $TAX_ID to get unique proc name (when running in parallel with create subs)
    #using proc name from command line when running solo
    my $proc = defined $param_href->{PROC} ? $param_href->{PROC} : "proc_create_phylo_$TAX_ID";
    $log->trace("Report: using procedure $proc!");
    my $table_phylo = "phylo_$TAX_ID";
    my $table_tree  = "tree$TAX_ID";
    my $table_ret   = "tree_fn_ret_ph$TAX_ID";

	#throws error: Commands out of sync; you can't run this command now
	#without changes to $dbh
	#$dbh->{mysql_use_result} = 1;
	$dbh->{mysql_server_prepare} = 0;   #procedures don't work with server side prepare
	my $dbh_trace = sprintf(Dumper tied %$dbh);
	$log->trace("$dbh_trace");

    #CALL proc_create_phylo$TAX_ID
    my $call_proc_query = qq{
    CALL $proc($TAX_ID);
    };
    eval { $dbh->do( $call_proc_query ) };
    $log->error( "Action: executing $proc failed: $@" ) if $@;
    $log->debug( "Action: procedure $proc executed successfully!" ) unless $@;

	#clean tables
	foreach my $table_name ($table_tree, $table_ret) {
	    my $drop_query = sprintf( qq{
	    DROP TABLE IF EXISTS %s
	    }, $dbh->quote_identifier($table_name) );
	    eval { $dbh->do($drop_query) };
	    $log->error("Action: dropping $table_name failed: $@") if $@;
	    $log->info("Action: $table_name dropped successfully!") unless $@;
	}
	
	$dbh->disconnect;

    return;
}

### INTERFACE SUB ###
# Usage      : run_mysqldump( $param_href );
# Purpose    : runs mysqldump from perl
# Returns    : nothing
# Parameters : ( $param_href)
# Throws     : croaks for parameters
# Comments   : run at end of procedure
# See Also   :
sub run_mysqldump {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('run_mysqldump() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $USER     = $param_href->{USER}     or $log->logcroak('no $USER specified on command line!');
    my $PASSWORD = $param_href->{PASSWORD} or $log->logcroak('no $PASSWORD specified on command line!');
    my $PORT     = $param_href->{PORT}     or $log->logcroak('no $PORT specified on command line!');
    my $SOCKET   = $param_href->{SOCKET}   or $log->logcroak('no $SOCKET specified on command line!');

    #get date for backup
    my $now  = DateTime::Tiny->now;
    my $date = $now->year . '_' . $now->month . '_' . $now->day;

    #my $datetime if doing more than one backup per hour
    #  = $now->year . '_' . $now->month . '_' . $now->day . '_' . $now->hour . '_' . $now->minute . '_' . $now->second;

    #get nice directory and name
    my $target_dry = path( $OUT, $DATABASE . "_schema_$date.sql" );
    my $target     = path( $OUT, $DATABASE . "_backup_$date.sql.gz" );
    $log->debug("This is mysqldump schema dump:$target_dry");
    $log->debug("This is mysqldump db file:$target");

    #commands to run
    my $cmd_dry = "mysqldump --socket=$SOCKET --port=$PORT -u $USER --password=$PASSWORD --databases $DATABASE ";
    $cmd_dry .= "--single-transaction --routines --triggers --events --no-data ";
    $cmd_dry .= "> $target_dry";
    my $cmd = "time mysqldump --socket=$SOCKET --port=$PORT -u $USER --password=$PASSWORD --databases $DATABASE ";
    $cmd .= "--single-transaction --routines --triggers --events ";
    $cmd .= "| pigz -c -1 > $target";

    #capture output of mysqldump command
    my ( $stdout_dry, $stderr_dry, $exit_dry ) = capture_output( $cmd_dry, $param_href );
    if ( $exit_dry == 0 ) {
        $log->info("Action: database $DATABASE schema backup succeeded at $target_dry");
    }
    else {
        $log->error("Action: database $DATABASE schema backup succeeded at $target_dry");
    }

    my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
    if ( $exit == 0 ) {
        $log->info("Action: database $DATABASE table backup succeeded at $target");
    }
    else {
        $log->error("Action: database $DATABASE table backup succeeded at $target");
    }

    return;
}


### INTERNAL UTILITY ###
# Usage      : save_cookie( $param_href );
# Purpose    : downloads cookie from JGI
# Returns    : nothing
# Parameters : needs $OUT
# Throws     : 
# Comments   : needed for jgi_download()
# See Also   : jgi_download()
sub save_cookie {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('save_cookie() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');

	my $cookie_path = path($OUT, 'cookie_jgi');

	#setup request
	my $url = q{https://signon.jgi.doe.gov/signon/create};
	my $user = q{msestak@irb.hr};
	my $pass = q{jgi_for_lifem8};

	my $cmd = qq{curl $url --data-ascii "login=$user&password=$pass" -c $cookie_path > /dev/null};
	my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
	    if ( $exit == 0 ) {
	        $log->info("Action: cookie from JGI saved at $cookie_path");
	    }
	    else {
	        $log->error("Action: failed to save cookie from JGI:\n$stderr");
	    }

	#modify cookie to handle another site (.jgi-psf.org) with fungi data
	open my $cookie_fh, "+<", $cookie_path or $log->logdie("Can't open $cookie_path:$!");
	my $site;
	while (<$cookie_fh>) {
		chomp;
		#say "COOKIE BEFORE:\n$_";
		if ( m{\A\.jgi\.doe\.gov(.+)\z} ){
			$site = $1;
			#say "SITE:$site";
			$site = '.jgi-psf.org' . $site;
		}
	}
	say {$cookie_fh} $site;
	close $cookie_fh;

	my $cookie_data = path($cookie_path)->slurp;
	$log->trace("COOKIE AFTER:\n$cookie_data");

    return;
}

### INTERNAL UTILITY ###
# Usage      : get_jgi_xml( $param_href );
# Purpose    : downloads xml file with locations of JGI genomes
# Returns    : $xml_name, $xml_path
# Parameters : needs $OUT $DATABASE
# Throws     : 
# Comments   : needed for jgi_download()
# See Also   : jgi_download()
sub get_jgi_xml {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('jgi_xml() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $URL      = $param_href->{URL}      or $log->logcroak('no $URL found in sub invocation!');
	my $cookie_path = path($OUT, 'cookie_jgi');
	(my $xml_name = $URL) =~ s{\A(?:.+?)organism=(.+)\z}{$1};
	my $xml_path = path($OUT, $xml_name . '.xml')->canonpath;

	my $cmd = qq{curl $URL -b $cookie_path -c $cookie_path > $xml_path};
	my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
	    if ( $exit == 0 ) {
	        $log->debug("Action: XML $xml_name from JGI saved at $xml_path");

			#check for zero size
			if (-z $xml_path) {
				$log->error("ZERO size: $xml_path");
			}
	    }
	    else {
	        $log->error("Action: failed to save $xml_name from JGI:\n$stderr");
	    }

    return $xml_name, $xml_path;
}

### INTERNAL UTILITY ###
# Usage      : get_jgi_genome( $param_href );
# Purpose    : downloads genome from JGI, inserts this info to jgi_download table
#            : and saves genome as taxid if found
# Returns    : nothing
# Parameters : needs $OUT
# Throws     : 
# Comments   : needed for jgi_download()
# See Also   : jgi_download()
sub get_jgi_genome {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('get_jgi_genome() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT         = $param_href->{OUT}         or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE    = $param_href->{DATABASE}    or $log->logcroak('no $DATABASE specified on command line!');
    my $NAMES       = $param_href->{NAMES}       or $log->logcroak('no $NAMES specified on command line!');
    my $LABEL       = $param_href->{LABEL}       or $log->logcroak('no $LABEL specified in sub!');
    my $FILENAME    = $param_href->{FILENAME}    or $log->logcroak('no $FILENAME specified in sub!');
    my $SIZE        = $param_href->{SIZE}        or $log->logcroak('no $SIZE specified in sub!');
    my $SIZEINBYTES = $param_href->{SIZEINBYTES} or $log->logcroak('no $SIZEINBYTES specified in sub!');
    my $TIMESTAMP   = $param_href->{TIMESTAMP}   or $log->logcroak('no $TIMESTAMP specified in sub!');
    my $PROJECT     = $param_href->{PROJECT}     or $log->logcroak('no $PROJECT specified in sub!');
    my $MD5         = $param_href->{MD5}         or $log->logcroak('no $MD5 specified in sub!');
    my $URL         = $param_href->{URL}         or $log->logcroak('no $URL specified in sub!');

    #get new handle
    my $dbh = dbi_connect($param_href);

	#search by species_name from filename
	my ($first_letter, $rest) = $FILENAME =~ m{\A(.)([^_]+).+\z};
	my $species_pattern = $first_letter . '%' . $rest;
	my $get_species_name = qq{
	SELECT species_name
	FROM $NAMES
	WHERE species_name LIKE '$species_pattern'
	};
	my @species = map { $_->[0] } @{ $dbh->selectall_arrayref($get_species_name) };

	#retrieve ti by species_name
	my $get_ti = qq{
	SELECT ti
	FROM $NAMES
	WHERE species_name = ?
	};
	my $sth = $dbh->prepare($get_ti);

	my ($ti, $species);
	if (scalar @species == 1) {
		($species) = @species;
		$sth->execute($species);
		$sth->bind_col(1, \$ti);
		$sth->fetchrow_arrayref();
		$log->info("SPECIES $species with ti:$ti");
	}
	else {
		$species = prompt "which species you want to retrieve",
			-menu => [@species],
			-number,
			">";
		$sth->execute($species);
		$sth->bind_col(1, \$ti);
		$sth->fetchrow_arrayref();
		$log->info("SPECIES $species with ti:$ti");
	}

        #insert all info into DB
        my $select_columns = qq{
			SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME = 'jgi_download' AND ORDINAL_POSITION > 1
		};
        my @columns = map { $_->[0] } @{ $dbh->selectall_arrayref($select_columns) };

        #prepare insert
        my $fieldlist = join ", ", @columns;
        my $field_placeholders = join ", ", map {'?'} @columns;
        my $insert_query = sprintf(qq{
		INSERT INTO jgi_download ( $fieldlist )
	    VALUES ( $field_placeholders )
		} );
        my $sth2 = $dbh->prepare($insert_query);

        my @col_loh;
        push @col_loh,
          { label        => $LABEL,
            filename     => $FILENAME,
            size         => $SIZE,
            sizeInBytes  => $SIZEINBYTES,
            timestamp    => $TIMESTAMP,
            project      => $PROJECT,
			md5          => $MD5,
            url          => $URL,
            ti           => $ti,
            species_name => $species,
          };

        #hash slice - values come in order of columns list
      INSERT:
        foreach (@col_loh) {
            eval { $sth2->execute( @{$_}{@columns} ) };
            if ($@) {
                my $species_error = $_->{species_name};
                $log->error(qq|Report: insert failed for:$species_error (duplicate genome with PRE?)|);

                #say $@;
                next INSERT;
            }
			else {
				$log->debug("$species inserted with:$ti");
			}
            #sth->execute( $_->{species_name}, $_->{ti}, $_->{assembly}, $_->{assembly_accession}, $_->{variation} );
        }

    return $ti;
}


### INTERNAL UTILITY ###
# Usage      : download_phytozome( $param_href );
# Purpose    : downloads genomes from Phytozome portion of JGI
# Returns    : nothing
# Parameters : $param_href
# Throws     : 
# Comments   : part of jgi_download mode
# See Also   : jgi_download()
sub download_phytozome {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('download_phytozome() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');

    #get new handle
    my $dbh = dbi_connect($param_href);

	my $URL = q{http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=PhytozomeV10};
	
	my ($xml_name, $xml_path) = get_jgi_xml( { URL => $URL, %{$param_href} } );

	my $twig= new XML::Twig(pretty_print => 'indented');
	$twig->parsefile( $xml_path );			# build the twig
	
	my $root= $twig->root;					# get the root of the twig
	my @folders_upper = $root->children;    # get the folders list
	
	UPPER:
	foreach my $folder_upper (@folders_upper) {
		my $species_name = $folder_upper->att( 'name' );
		$log->debug("FOLDER_UPPER-NAME:{$species_name}");

		#skip unwanted divisions and go into early_release folder
		foreach ($species_name) {
			when (/global_analysis/) { $log->trace("Action: skipped upper_folder $species_name") and next UPPER; }
			when (/orthology/) { $log->trace("Action: skipped upper_folder $species_name") and next UPPER; }
			when (/inParanoid/) { $log->trace("Action: skipped upper_folder $species_name") and next UPPER; }
			when (/early_release/) {
				my @early_folders_upper = $folder_upper->children;
				$log->warn("Action: working in $species_name");
				foreach my $early_folder_upper (@early_folders_upper) {
					my $early_species_name = $early_folder_upper->att( 'name' );
					say "EARLY_FOLDER_UPPER-NAME:{$early_species_name}";
					
					#my @early_folders= $early_folder_upper->children;
					#say "LISTING EARLY_FOLDERS:@early_folders";

					list_folders( {FOLDER => $early_folder_upper, %{$param_href} } );
				}
			}
			when(/.+/) {
				list_folders( { FOLDER => $folder_upper,  %{$param_href} } );
			}
		}
	
	}
}

### INTERNAL UTILITY ###
# Usage      : list_folders( $param_href );
# Purpose    : lists folders with species and grabs files from them
# Returns    : hash ref of params for get_jgi_genome()
# Parameters : $param_href
# Throws     : 
# Comments   : part of jgi_download mode
# See Also   : jgi_download()
sub list_folders {
	my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('list_folders() needs a $folder_upper') unless @_ == 1;
    my ($param_href) = @_;

    my $folder_upper = $param_href->{FOLDER}  or $log->logcroak('no $OUT specified on command line!');

	#unwind folders to get to genomes
	my @folders= $folder_upper->children;
	#say "LISTING FOLDERS:@folders";
	
	FOLDER:
	foreach my $folder (@folders) {
		my $division_name = $folder->att( 'name' );
		if (! defined $division_name) {
			$log->trace("Action: skipped empty folder") and next FOLDER;
		}
		else {
			#say "FOLDER-NAME:{$division_name}";
		}

		#list of divisions to skip
		foreach ($division_name) {
			when (/assembly/) { $log->trace("Action: skipped folder $division_name") and next FOLDER; }
			when (/diversity/) { $log->trace("Action: skipped folder $division_name") and next FOLDER; }
			when (/bam/) { $log->trace("Action: skipped folder $division_name") and next FOLDER; }
			when (/expression/) { $log->trace("Action: skipped folder $division_name") and next FOLDER; }
		}

        #real work here
        my @files = $folder->children;

        #say "LISTING FILES:@files";
        foreach my $file (@files) {
            my $filename = $file->att('filename');
            if ( $filename =~ m{protein.fa.gz\z}g ) {
                my $label = $file->att('label');
				#say "label:$label";
				#say "filename:$filename";
                my $size = $file->att('size');
				#say "size:$size";
                my $size_in_bytes = $file->att('sizeInBytes');
				#say "sizeInBytes:$size_in_bytes";
                my $timestamp = $file->att('timestamp');
				#say "timestamp:$timestamp";
                my $project = $file->att('project');
				$project = $label if $project eq '';
				#say "project:$project";
                my $md5 = $file->att('md5');
				#say "md5:$md5";
                my $url = $file->att('url');
				#say "url:$url";
                $url =~ s{/ext-api(?:.+?)url=(.+)}{$1};
				#say $url;
                $url = 'http://genome.jgi.doe.gov' . $url;
				#say $url;
                get_jgi_genome(
                    {   LABEL       => $label,
                        FILENAME    => $filename,
                        SIZE        => $size,
                        SIZEINBYTES => $size_in_bytes,
                        TIMESTAMP   => $timestamp,
                        PROJECT     => $project,
						MD5         => $md5,
                        URL         => $url,
                        %{$param_href}
                    }
                );
            }

        }
	}
}


### INTERFACE SUB ###
# Usage      : nr_genome_counts( $param_href );
# Purpose    : gets gene count of all tis (species and other categories) from nr table
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : 
# See Also   : 
sub nr_genome_counts {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'nr_genome_counts() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} };
	my $NR_TABLE = $TABLES{nr};
	my $NAMES_TABLE = $TABLES{names};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#select nr table for calculation of counts
	my $nr_tbl;
	if ($NR_TABLE) {
		$nr_tbl = $NR_TABLE;
    	$log->trace( "Action: using: $nr_tbl from command line" );
	}
	else {
		#first prompt to select nr_base table
    	my $select_tables = qq{
    	SELECT TABLE_NAME 
    	FROM INFORMATION_SCHEMA.TABLES
    	WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME LIKE 'nr%'
    	};
    	my @tables = map { $_->[0] } @{ $dbh->selectall_arrayref($select_tables) };
    	
    	#ask to choose NR_TI_GI_FASTA table
    	my $table_nr = prompt 'Select NR_TI_GI_FASTA table to use ',
    	  -menu => [ @tables ],
		  -number,
    	  '>';
		$nr_tbl = $table_nr;
    	$log->trace( "Action: using: $nr_tbl from prompt" );
	}

	#select names table for calculation of counts
	my $na_tbl;
	if ($NAMES_TABLE) {
		$na_tbl = $NAMES_TABLE;
    	$log->trace( "Action: using: $na_tbl from command line" );
	}
	else {
    	my $select_tables = qq{
    	SELECT TABLE_NAME 
    	FROM INFORMATION_SCHEMA.TABLES
    	WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME LIKE 'names%'
    	};
    	my @tables = map { $_->[0] } @{ $dbh->selectall_arrayref($select_tables) };
    	
    	#ask to choose NAMES table
    	my $table_na = prompt 'Select NAMES table to use ',
    	  -menu => [ @tables ],
		  -number,
    	  '>';
		$na_tbl = $table_na;
    	$log->trace( "Action: using: $na_tbl from prompt" );
	}

    #create ti genomes count table
	my $nr_cnt_tbl = "${nr_tbl}_cnt";
    my $create_query_cnt = sprintf( qq{
    CREATE TABLE %s (
    ti INT UNSIGNED NOT NULL,
    genes_cnt INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NULL,
    PRIMARY KEY(ti),
	KEY(genes_cnt),
	KEY(species_name)
    )ENGINE=$ENGINE CHARSET=ascii }, $dbh->quote_identifier($nr_cnt_tbl) );
	create_table( { TABLE_NAME => $nr_cnt_tbl, DBH => $dbh, QUERY => $create_query_cnt, %{$param_href} } );

	#INSERT all species
    my $insert_query_cnt = qq{
    INSERT INTO $nr_cnt_tbl (ti, genes_cnt)
    SELECT ti, COUNT(ti) AS genes_cnt
    FROM $nr_tbl
	GROUP BY ti
    };
    eval { $dbh->do($insert_query_cnt, { async => 1 } ) };

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
	my $rows_cnt = $dbh->mysql_async_result;
    $log->debug( "Action: import to $nr_cnt_tbl inserted $rows_cnt rows!" ) unless $@;
    $log->error( "Action: loading $nr_cnt_tbl failed: $@" ) if $@;

	#UPDATE with species_names
    my $update_query_cnt = qq{
	UPDATE $nr_cnt_tbl AS nr
	SET nr.species_name = (SELECT DISTINCT na.species_name
	FROM $na_tbl AS na WHERE nr.ti = na.ti);
    };
    eval { $dbh->do($update_query_cnt, { async => 1 } ) };
	my $rows_up = $dbh->mysql_async_result;
    $log->debug( "Action: update to $nr_cnt_tbl updated $rows_up rows!" ) unless $@;
    $log->error( "Action: updating $nr_cnt_tbl failed: $@" ) if $@;

	sleep 1;

	#delete all names with single part (genus, division, ...) and keep species names Homo_sapiens
	#first get all species_names
	my $species_query = qq{
    SELECT species_name 
    FROM $nr_cnt_tbl
    ORDER BY species_name
    };
    my @species_names = map { $_->[0] } @{ $dbh->selectall_arrayref($species_query) };

	SPECIES:
	foreach my $taxon (@species_names) {
		if (! defined $taxon) {
			$log->warn("Report: NULL species_name found");
			next SPECIES;
		}
		if ($taxon =~ m{\A(?:[^_]+)_(?:.+)\z}g) {
			next SPECIES;
		}
		else {
			my $delete_cnt = qq{
			DELETE nr FROM $nr_cnt_tbl AS nr
			WHERE species_name = '$taxon'
	    	};
	    	eval { $dbh->do($delete_cnt, { async => 1 } ) };
			my $rows_del = $dbh->mysql_async_result;
	    	$log->debug( "Action: table $nr_cnt_tbl deleted $rows_del rows for {$taxon}" ) unless $@;
	    	$log->error( "Action: deleting $nr_cnt_tbl failed for {$taxon}: $@" ) if $@;

		}
	}



	$dbh->disconnect;

	return;
}

### INTERFACE SUB ###
# Usage      : export_all_nr_genomes( $param_href );
# Purpose    : prints all nr genomes to dropdox/D.../db../data/eukarya
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it extracts genomes from database and prints them to $OUT
# See Also   : 
sub export_all_nr_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'export_nr_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );
	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} };
	my $NR_TABLE = $TABLES{nr};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#select nr_cnt table to get tis to export
	my $nr_cnt_tbl;
	if ($NR_TABLE) {
		$nr_cnt_tbl = $NR_TABLE;
    	$log->trace( "Action: using: $nr_cnt_tbl from command line" );
	}
	else {
    	my $select_tables = qq{
    	SELECT TABLE_NAME 
    	FROM INFORMATION_SCHEMA.TABLES
    	WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME LIKE 'nr%'
    	};
    	my @tables = map { $_->[0] } @{ $dbh->selectall_arrayref($select_tables) };
    	
    	#ask to choose NR_TI_GI_FASTA table
    	my $table_nr = prompt 'Select NR_TI_GI_FASTA_CNT table to use ',
    	  -menu => [ @tables ],
		  -number,
    	  '>';
		$nr_cnt_tbl = $table_nr;
    	$log->trace( "Action: using: $nr_cnt_tbl from prompt" );
	}

	#get all tax_ids that belong to nr and print them to $OUT
    my $tis_query = qq{
    SELECT ti
    FROM $nr_cnt_tbl
	WHERE genes_cnt >= 2000
    ORDER BY ti
    };
    my @tis = map { $_->[0] } @{ $dbh->selectall_arrayref($tis_query) };

	(my $nr_tbl = $nr_cnt_tbl) =~ s{\A(.+)_cnt}{$1};
	#starting iteration over @tis to extract genomes from nr_ti_gi_fasta_cnt table and print to $OUT
	foreach my $ti (@tis) {
		$log->trace( "Action: working on $ti" );
		my $genome_out = path( $OUT, $ti);
		if (-f $genome_out) {
			unlink $genome_out and $log->trace( "Action: file $genome_out unlinked" );
		}

		my $genome_query = qq{
		SELECT CONCAT('>', gi), fasta
		INTO OUTFILE '$genome_out'
		FIELDS TERMINATED BY '\n'
		LINES TERMINATED BY '\n'
		FROM $nr_tbl
		WHERE ti = $ti
		ORDER BY gi;
		};

		eval { $dbh->do($genome_query) };
		$log->error( "Action: file $genome_out failed to export: $@" ) if $@;
		$log->debug( "Action: file $genome_out exported" ) unless $@;
	}

	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : get_existing_ti( $param_href );
# Purpose    : collects tax_ids from tiktaalik and inserts it as a table in db
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : 
# See Also   : 
sub get_existing_ti {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'get_existing_ti() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $IN       = $param_href->{IN}       or $log->logcroak('no $IN specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} };
    my $NAMES_TABLE = $TABLES{names};

	#collect genomes from $IN
	my @ti_files = File::Find::Rule->file()
								   ->name(qr/\A\d+\z/)
								   ->in($IN);
	my @tis;
	foreach my $file (@ti_files) {
		my $ti = path($file)->basename;
		push @tis, $ti;
	}
	my @tis_sorted = sort {$a <=> $b} @tis;

	#get new database handle
	my $dbh = dbi_connect($param_href);

	#insert tax_ids into the database
	my $table_ti = "ti_files";
    my $create_query = qq{
    CREATE TABLE $table_ti (
    ti INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NULL,
    PRIMARY KEY(ti),
	KEY(species_name)
    )ENGINE=$ENGINE CHARSET=ascii };
    eval { $dbh->do($create_query) };
	create_table( { TABLE_NAME => $table_ti, DBH => $dbh, QUERY => $create_query, %{$param_href} } );


    #now insert in single transaction (performance better)
	$dbh->{AutoCommit} = 0;    # enable transactions, if possible
    eval {
        my $insert_query = qq{
        INSERT INTO $table_ti (ti)
		VALUES (?)
        };
        my $sth = $dbh->prepare($insert_query);
		foreach my $ti (@tis_sorted) {
			$sth->execute($ti);
		}

        $dbh->commit;          # commit the changes if we get this far
		$log->debug( "Table $table_ti inserted successfully!");
    };
    if ($@) {
        $log->logcarp( "Transaction aborted because $@" );

        # now rollback to undo the incomplete changes
        # but do it in an eval{} as it may also fail
        eval { $dbh->rollback };

        # add other application on-error-clean-up code here
        if ($@) {
            $log->logcarp( "Inserting into $table_ti failed! Transaction failed to commit and failed to rollback!" );
        }
    }

	#UPDATE with species_names
	$dbh->{AutoCommit} = 1;    # disable transactions
    my $update_query = qq{
	UPDATE $table_ti AS ti
	SET ti.species_name = (SELECT DISTINCT na.species_name
	FROM $NAMES_TABLE AS na WHERE ti.ti = na.ti);
    };
    eval { $dbh->do($update_query, { async => 1 } ) };
	my $rows = $dbh->mysql_async_result;
    $log->debug( "Action: update to $table_ti updated $rows rows!" );

    $log->error( "Action: updating $table_ti failed: $@" ) if $@;
    $log->debug( "Action: table $table_ti updated successfully!" ) unless $@;

	#update for Hydra magnipapillata and Caenorhabditis briggsae
    my %species = (
        473542 => 'Caenorhabditis briggsae',
        6085   => 'Hydra magnipapillata',
    );
	while (my ($ti, $species_name) = each %species) {
		my $update_q = qq{
		UPDATE $table_ti
		SET species_name = '$species_name'
		WHERE ti = $ti
		};
		eval { $dbh->do($update_q, { async => 1 } ) };
		my $rows_up = $dbh->mysql_async_result;
    	$log->debug( "Action: update to $table_ti for $species_name updated $rows_up rows!" ) unless $@;
    	$log->error( "Action: updating $table_ti for $species_name failed: $@" ) if $@;
	}

	$dbh->disconnect;

	return;

}


### INTERFACE SUB ###
# Usage      : get_missing_genomes( $param_href );
# Purpose    : JOINs existing tis with all possible tis from nr_base (ti, gi, fasta)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : 
# See Also   : 
sub get_missing_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'get_missing_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE       = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $DATABASE     = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES       = %{ $param_href->{TABLES} };
    my $NR_CNT_TBL   = $TABLES{nr_cnt};
    my $TI_FILES_TBL = $TABLES{ti_files};

    #get new handle
    my $dbh = dbi_connect($param_href);

	#DELETE genomes already present in database
	my $delete_cnt = qq{
	DELETE nr FROM $NR_CNT_TBL AS nr
	INNER JOIN $TI_FILES_TBL AS ti
	ON nr.ti = ti.ti
    };
    eval { $dbh->do($delete_cnt, { async => 1 } ) };
	my $rows_del = $dbh->mysql_async_result;
    $log->debug( "Table $NR_CNT_TBL deleted $rows_del rows!" ) unless $@;
    $log->error( "Deleting $NR_CNT_TBL failed: $@" ) if $@;

	$dbh->disconnect;

	return;
}

### INTERFACE SUB ###
# Usage      : del_virus_from_nr( $param_href );
# Purpose    : deletes viroids, viruses, other and unclassified sequences from gi_ti or nr_ti_gi tables
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it needs phylo tables for these sequences created by call_phylo (and friends)
# See Also   : call_phylo()
sub del_virus_from_nr {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_virus_from_nr() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_TBL   = $TABLES{nr};

    #get new handle
    my $dbh = dbi_connect($param_href);

	#DELETE nr table based on phylo tables of viruses, viroids,other and unclassified sequences
    my %tis_to_del = (
        12884 => 'Viroids',
        10239 => 'Viruses',
        28384 => 'other sequences',
        12908 => 'unclassified sequences',
    );

	while (my ($ti, $division) = each %tis_to_del) {
	
		my $phylo_tbl = 'phylo_' . $ti;
	
		my $delete_cnt = qq{
		DELETE nr FROM $NR_TBL AS nr
		INNER JOIN $phylo_tbl AS ph
		ON nr.ti = ph.ps1
		};
		eval { $dbh->do($delete_cnt, { async => 1 } ) };
	
		#check status while running
		{    
		    my $dbh_check         = dbi_connect($param_href);
		    until ( $dbh->mysql_async_ready ) {
		        my $processlist_query = qq{
		        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
		        WHERE DB = ? AND INFO LIKE 'DELETE%';
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
	
		my $rows_del = $dbh->mysql_async_result;
		$log->debug( "Table $NR_TBL deleted $rows_del rows for {$division}" ) unless $@;
		$log->error( "Deleting $NR_TBL failed for {$division}: $@" ) if $@;
	}
	
	#slower
	#while (my ($ti, $division) = each %tis_to_del) {

	#	my $phylo_tbl = 'phylo_' . $ti;

	#	my $delete_cnt = qq{
	#	DELETE nr FROM $NR_TBL AS nr
	#	WHERE EXISTS (
	#		SELECT 1 
	#		FROM $phylo_tbl AS ph
	#		WHERE nr.ti = ph.ps1)
    #	};
    #	eval { $dbh->do($delete_cnt, { async => 1 } ) };

	#	#check status while running
	#	{    
    #	    my $dbh_check         = dbi_connect($param_href);
    #	    until ( $dbh->mysql_async_ready ) {
    #	        my $processlist_query = qq{
    #	        SELECT TIME, STATE FROM INFORMATION_SCHEMA.PROCESSLIST
    #	        WHERE DB = ? AND INFO LIKE 'DELETE%';
    #	        };
    #	        my $sth = $dbh_check->prepare($processlist_query);
    #	        $sth->execute($DATABASE);
    #	        my ( $time, $state );
    #	        $sth->bind_columns( \( $time, $state ) );
    #	        while ( $sth->fetchrow_arrayref ) {
    #	            my $process = sprintf( "Time running:%0.3f sec\tSTATE:%s\n", $time, $state );
    #	            $log->trace( $process );
    #	            sleep 10;
    #	        }
    #	    }
    #	}    #end check

	#	my $rows_del = $dbh->mysql_async_result;
    #	$log->debug( "Table $NR_TBL deleted $rows_del rows for {$division}" ) unless $@;
    #	$log->error( "Deleting $NR_TBL failed for {$division}: $@" ) if $@;
	#}

	$dbh->disconnect;

	return;
}

### INTERFACE SUB ###
# Usage      : del_nr_genomes( $param_href );
# Purpose    : deletes species NR genomes that have subspecies or strain genomes (first step)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it works on nr set of genomes only (first step) from nr_ti_gi_cnt table
# See Also   : del_total_genomes() ->second step
sub del_nr_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_nr_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE}    or $log->logcroak( 'no $DATABASE specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_CNT_TBL   = $TABLES{nr_cnt};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#get all species names as array
    my $species_query = qq{
    SELECT species_name 
    FROM $NR_CNT_TBL
    ORDER BY species_name
    };
    my @species_names = map { $_->[0] } @{ $dbh->selectall_arrayref($species_query) };

	say join("\n", @species_names);
	#get count of genus if there is more than 1 (into hash)
	my ($found_genus, %found_hash_cnt);
	foreach my $species (@species_names) {
		(my $genus = $species) =~ s/\A([^_]+)_(?:.+)\z/$1/g;
		say "SPECIES:$species\tGENUS:$genus";

		if ($genus eq $found_genus) {
			$found_hash_cnt{$genus}++;   #add to hash only if found earlier (only duplicates)
			#say "GENUS:$genus EQ FOUND:$found_genus";
		}
		$found_genus = $genus;   #now found has previous genus
	}
	#print Dumper(\%found_hash_cnt);
	#$VAR1 = {
	#          'Aphanomyces' => 1
	#        };

	#now get found species into HoHoA to display them later
	my (%hohoa);
	foreach my $species2 (@species_names) {
		(my $genus2 = $species2) =~ s/\A([^_]+)_(?:.+)\z/$1/g;
		#say "GENUS2:$genus2";
		if (exists $found_hash_cnt{$genus2}) {
			push @{ $hohoa{$genus2}{ $found_hash_cnt{$genus2} } }, $species2;   #created HoHoArrays

		}
	}	
	#print Dumper(\%hohoa);
	#$VAR1 = {
	#          'Aphanomyces' => {
	#                             '1' => [
	#                                      'Aphanomyces_astaci',
	#                                      'Aphanomyces_invadans'
	#                                    ]
	#                           }
	#        };

	#use HoHoA to display species to delete
	while (my ($key, $inner_hash) = each (%hohoa)) {   #$inner_hash is a ref
		say Dumper( $inner_hash);
		#$VAR1 = {
		#          '1' => [
		#                   'Aphanomyces_astaci',
		#                   'Aphanomyces_invadans'
		#                 ]
		#        };

		INNER:
		foreach my $cnt (keys %{$inner_hash} ) {
			#say "$cnt: @{ $inner_hash->{$cnt}    }";

			#first prompt to see if there is anything to delete (printed by Dumper earlier)
			my $continue = prompt( "Delete? ", -yn1 );
			say $continue;
            if ( $continue eq 'y' ) {
				#ask to choose species to delete
				my $species_delete = prompt 'Choose which SPECIES you want to DELETE (single num)',
				-menu => [ @{ $inner_hash->{$cnt} } ],
				-number,
				'>';
				$log->trace( "DELETING: $species_delete" );

				#DELETE species from nr_base_eu_cnt table (because it is species with strains present)
				my $delete_species = qq{
				DELETE nr FROM $NR_CNT_TBL AS nr
				WHERE species_name = ('$species_delete');
			    };
			    eval { $dbh->do($delete_species, { async => 1 } ) };
				my $rows_del_spec = $dbh->mysql_async_result;
			    $log->debug( "Table $NR_CNT_TBL deleted $rows_del_spec rows with $species_delete" ) unless $@;
			    $log->debug( "Deleting $NR_CNT_TBL failed: $@" ) if $@;

				#prompt to redo loop (for multiple delete on same genus)
				my $redo = prompt( "Redo?", -yns );
				if ($redo eq 'y') {
					redo INNER;
				}
				else {
					next INNER;
				}
            }
            elsif ($continue eq 'n') {
				$log->trace( "In genus $key there is nothing to DELETE!" );
                next INNER;
            }
		}
	}

	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : del_total_genomes( $param_href );
# Purpose    : deletes TOTAL species genomes that have subspecies or strain genomes (second step)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : first it creates ti_fulllist table to hold all genomes
#            : it deletes genomes that have species and strain genomes in full dataset
#            : (both nr and existing genomes)
# See Also   : second step
sub del_total_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_total_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $DATABASE = $param_href->{DATABASE}    or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_CNT_TBL   = $TABLES{nr_cnt};
    my $TI_FILES_TBL = $TABLES{ti_files};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info( "---------->JOIN-ing two tables: $NR_CNT_TBL and $TI_FILES_TBL" );

    #create table
	my $table_list = "ti_fulllist";
    my $create_query = qq{
    CREATE TABLE $table_list (
    ti INT UNSIGNED NOT NULL,
    genes_cnt INT UNSIGNED NULL,
	species_name VARCHAR(200) NULL,
    PRIMARY KEY(ti),
	KEY(genes_cnt),
	KEY(species_name)
    )ENGINE=$ENGINE CHARSET=ascii
    };
	create_table( { TABLE_NAME => $table_list, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

    my $insert_nr = qq{
    INSERT INTO $table_list (ti, genes_cnt, species_name)
    SELECT ti, genes_cnt, species_name
    FROM $NR_CNT_TBL
	ORDER BY ti
    };
    eval { $dbh->do($insert_nr, { async => 1 } ) };
    my $rows = $dbh->mysql_async_result;
    $log->debug( "Action: import inserted $rows rows!" ) unless $@;
    $log->error( "Action: loading $table_list failed: $@" ) if $@;

    my $insert_ti = qq{
    INSERT INTO $table_list (ti, species_name)
    SELECT ti, species_name
	FROM $TI_FILES_TBL
	ORDER BY ti
    };
    eval { $dbh->do($insert_ti, { async => 1 } ) };
    my $rows2 = $dbh->mysql_async_result;
    $log->debug( "Action: import inserted $rows2 rows!" ) unless $@;
    $log->error( "Action: loading $table_list failed: $@" ) if $@;

	#SECOND PART: remove superfluous genomes
	#get all species names as array
    my $species_query = qq{
    SELECT species_name 
    FROM $table_list
    ORDER BY species_name
    };
    my @species_names = map { $_->[0] } @{ $dbh->selectall_arrayref($species_query) };

	#get count of genus if there is more than 1 (into hash)
	my ($found_genus, %found_hash_cnt);
	foreach my $species (@species_names) {
		(my $genus = $species) =~ s/\A([^_]+)_(?:.+)\z/$1/g;
		#say "GENUS:$genus";

		if ($genus eq $found_genus) {
			$found_hash_cnt{$genus}++;   #does not track single species genera
			#say "GENUS:$genus EQ FOUND:$found_genus";
		}
		$found_genus = $genus;   #now found has previous genus
	}
	#print Dumper(\%found_hash_cnt);
	#$VAR1 = {
	#          'Aphanomyces' => 1
	#        };

	#now get found species into HoHoA to display them later
	my (%hohoa);
	foreach my $species2 (@species_names) {
		(my $genus2 = $species2) =~ s/\A([^_]+)_(?:.+)\z/$1/g;
		#say "GENUS2:$genus2";
		if (exists $found_hash_cnt{$genus2}) {
			push @{ $hohoa{$genus2}{ $found_hash_cnt{$genus2} } }, $species2;   #created HoHoArrays
		}
	}	
	#print Dumper(\%hohoa);
	#$VAR1 = {
	#          'Aphanomyces' => {
	#                             '1' => [
	#                                      'Aphanomyces_astaci',
	#                                      'Aphanomyces_invadans'
	#                                    ]
	#                           }
	#        };

	#use HoHoA to display species to delete
	while (my ($key, $inner_hash) = each (%hohoa)) {   #$inner_hash is a ref
		say Dumper( $inner_hash);
		#$VAR1 = {
		#          '1' => [
		#                   'Aphanomyces_astaci',
		#                   'Aphanomyces_invadans'
		#                 ]
		#        };

		INNER:
		foreach my $cnt (keys %{$inner_hash} ) {
			#say "$cnt: @{ $inner_hash->{$cnt}    }";

			#first prompt to see if there is anything to delete
			my $continue = prompt( "Delete? ", -yn1 );
			say $continue;
            if ( $continue eq 'y' ) {
				#ask to choose species to delete
				my $species_delete = prompt 'Choose which SPECIES you want to DELETE (single num)',
				-menu => [ @{ $inner_hash->{$cnt} } ],
				-number,
				'>';
				$log->trace( "DELETING: $species_delete" );

				#DELETE species from ti_full_list table (because it has species with strains present)
				#it accepts one num
				my $delete_species = qq{
				DELETE nr FROM $table_list AS nr
				WHERE species_name = ('$species_delete')
			    };
			    eval { $dbh->do($delete_species, { async => 1 } ) };
				my $rows_del_spec = $dbh->mysql_async_result;
			    $log->debug( "Action: table $table_list deleted $rows_del_spec rows with $species_delete" ) unless $@;
			    $log->error( "Action: deleting $NR_CNT_TBL failed: $@" ) if $@;
				
				#prompt to redo loop (for multiple delete on same genus)
				my $redo = prompt( "Redo?", -yns );
				if ($redo eq 'y') {
					redo INNER;
				}
				else {
					next INNER;
				}
            }
            elsif ($continue eq 'n') {
				$log->trace( "In genus $key there is nothing to DELETE!" );
                next INNER;
            }
		}
	}

	$dbh->disconnect;

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

 Part III -> load nr into database:

 perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if ./nr/nr_10k.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=gi_taxid -if ./nr/gi_taxid_prot.dmp.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm -mode=del_virus_from_nr -tbl nr=gi_taxid_prot_TokuDB -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v

 perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr -ho localhost -u msandbox -p msandbox --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=mysqldump -o ./t/nr -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 Part IV -> set phylogeny for focal species:

 perl ./lib/CollectGenomes.pm --mode=import_names -if ./nr/names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=import_nodes -if ./nr/nodes_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 2759 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 7955 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=InnoDB

 (Viroids:12884)=perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 12884 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
 (Viruses:10239)=perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 10239 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
 (Other:28384)=perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 28384 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
 (unclassified:12908)=perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 12908 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB

 Part V -> get genomes from nr base:

 perl ./lib/CollectGenomes.pm --mode=nr_genome_counts --tables nr=nr_ti_gi_fasta_InnoDB --tables names=names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=export_all_nr_genomes -o ./nr/ --tables nr=nr_ti_gi_fasta_InnoDB_cnt -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v

 Part VI -> combine nr genomes with Ensembl genomes and prin them out:

 perl ./lib/CollectGenomes.pm --mode=get_existing_ti --in=./ensembl_ftp/ --tables names=names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -en=InnoDB

 perl ./lib/CollectGenomes.pm --mode=get_missing_genomes --tables nr_cnt=nr_ti_gi_fasta_InnoDB_cnt -tbl ti_files=ti_files -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=InnoDB

 perl ./lib/CollectGenomes.pm --mode=del_nr_genomes -tbl nr_cnt=nr_ti_gi_fasta_InnoDB_cnt -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./lib/CollectGenomes.pm --mode=del_total_genomes -tbl nr_cnt=nr_ti_gi_fasta_InnoDB_cnt -tbl ti_files=ti_files -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625n=InnoDB


 perl ./bin/CollectGenomes.pm --mode=del_total_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=print_nr_genomes --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=copy_existing_genomes --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya_old/  --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 Part VII -> download genomes from JGI: (not working)

 perl ./lib/CollectGenomes.pm --mode=jgi_download --names=names_martin7 -o ./xml/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v

 Part VIII -> prepare and run cd-hit
 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=./data_in/t_eukarya/ --out=./data_out/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_29_07_15/data/archaea/ --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


 perl ./bin/CollectGenomes.pm --mode=run_cdhit --in=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock -v

 Part VIII -> prepare BLAST and run it:

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
 del_nr_genomes          => \&del_nr_genomes,
 del_total_genomes           => \&del_total_genomes,
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
