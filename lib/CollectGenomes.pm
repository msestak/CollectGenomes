#!/usr/bin/env perl
package CollectGenomes;

use 5.010001;
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
	create_db
	make_db_dirs
	create_table
	capture_output
	ftp_robust
	extract_and_load_nr
    extract_and_load_gi_taxid
	del_virus_from_nr
	del_missing_ti
	ti_gi_fasta
	get_ensembl_genomes
	import_names
	import_raw_names
	import_raw_nodes
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
	del_species_with_strain
	print_nr_genomes
	merge_existing_genomes
	copy_external_genomes
	copy_jgi_genomes
	ensembl_ftp
	ensembl_ftp_vertebrates
	prepare_cdhit_per_phylostrata
	run_cdhit
	cdhit_merge
	del_after_analyze
	manual_add_fasta
	
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

    #first capture parameters to enable VERBOSE flag for logging
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
    my $TAXID   = $param_href->{TAXID};
    my $VERBOSE  = $param_href->{VERBOSE};

	#start logging for the rest of program (without capturing of parameters)
    init_logging($VERBOSE);
    ##########################
    # ... in some function ...
    ##########################
    my $log = Log::Log4perl::get_logger("main");
    # Logs both to Screen and File appender
    $log->info("This is start of logging for $0");

    #get dump of param_href if -v (VERBOSE) flag is on (for debugging)
    my $dump_print = sprintf( Dumper($param_href) ) if $VERBOSE;
    $log->debug( '$param_href = ', "$dump_print" ) if $VERBOSE;


    #need to create dispatch table for different usage depending on mode requested
    #dispatch table is hash (could be also hash_ref)
    my %dispatch = (
		make_db_dirs                  => \&make_db_dirs,
        create_db                     => \&create_db,
        nr_ftp                        => \&ftp_robust,
        extract_and_load_nr           => \&extract_and_load_nr,
        gi_taxid                      => \&extract_and_load_gi_taxid,
		del_virus_from_nr             => \&del_virus_from_nr,
		del_missing_ti                => \&del_missing_ti,
        import_names                  => \&import_names,
        import_nodes                  => \&import_nodes,
		import_raw_names              => \&import_raw_names,
		import_raw_nodes              => \&import_raw_nodes,
        ti_gi_fasta                   => \&ti_gi_fasta,
        mysqldump                     => \&run_mysqldump,
        fn_tree                       => \&fn_create_tree,
        fn_retrieve                   => \&fn_retrieve_phylogeny,
        prompt_ph                     => \&prompt_fn_retrieve,
        proc_phylo                    => \&proc_create_phylo,
        call_phylo                    => \&call_proc_phylo,
		jgi_download                  => \&jgi_download,
        get_ensembl_genomes           => \&get_ensembl_genomes,
        nr_genome_counts              => \&nr_genome_counts,
		export_all_nr_genomes         => \&export_all_nr_genomes,
        get_missing_genomes           => \&get_missing_genomes,
        del_nr_genomes                => \&del_nr_genomes,
        del_total_genomes             => \&del_total_genomes,
		del_species_with_strain       => \&del_species_with_strain,
        print_nr_genomes              => \&print_nr_genomes,
        merge_existing_genomes        => \&merge_existing_genomes,
		copy_external_genomes         => \&copy_external_genomes,
		copy_jgi_genomes              => \&copy_jgi_genomes,
        ensembl_vertebrates           => \&ensembl_ftp_vertebrates,
        ensembl_ftp                   => \&ensembl_ftp,
        prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
        run_cdhit                     => \&run_cdhit,
		cdhit_merge                   => \&cdhit_merge,
		del_after_analyze             => \&del_after_analyze,
		manual_add_fasta              => \&manual_add_fasta,

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

    #no logger here

    #print command line arguments
    print 'My @ARGV: {', join( "} {", @ARGV ), '}', "\n";
	#<<< notidy
    my ($help,  $man,      @MODE,
		$NODES, $NAMES,    %TABLES,   $ORG,      $TAXID, $MAP,
		$OUT,   $IN,       $OUTFILE,  $INFILE,
        $HOST,  $DATABASE, $USER,     $PASSWORD, $PORT,   $SOCKET, $CHARSET, $ENGINE,
		$REMOTE_HOST,      $REMOTE_DIR,          $REMOTE_FILE,
    );
	#>>>
    my $VERBOSE = 0;    #default false (silent or here INFO log level)

    GetOptions(
        'help|h'           => \$help,
        'man|m'            => \$man,
        'mode|mo=s{1,}'    => \@MODE,          #accepts 1 or more arguments
        'nodes|no=s'       => \$NODES,
        'names|na=s'       => \$NAMES,
        'map=s'            => \$MAP,
        'tables|tbl=s'     => \%TABLES,        #accepts 1 or more arguments
        'organism|org=s'   => \$ORG,
        'tax_id|t=i'       => \$TAXID,
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
        'verbose+'      => \$VERBOSE,       #flag
    ) or pod2usage( -verbose => 1 );

    say "Printing {@MODE} before";
    @MODE = split( /,/, join( ',', @MODE ) );
    say "Printing {@MODE} after";

    pod2usage( -verbose => 1 ) if $help;
    pod2usage( -verbose => 2 ) if $man;

    die 'No @MODE specified on command line' unless @MODE;

    if ($OUT) {
        say 'My output path: ', path($OUT);
        $OUT = path($OUT)->absolute->canonpath;
        say 'My absolute output path: ', path($OUT);
    }
    if ($IN) {
        say 'My input path: ', path($IN);
        $IN = path($IN)->absolute->canonpath;
        say 'My absolute input path: ', path($IN);
    }
    if ($OUTFILE) {
        say 'My output file: ', path($OUTFILE);
        $OUTFILE = path($OUTFILE)->absolute->canonpath;
        say 'My absolute output file: ', path($OUTFILE);
    }
    if ($INFILE) {
        say 'My input file: ', path($INFILE);
        $INFILE = path($INFILE)->absolute->canonpath;
        say 'My absolute input file: ', path($INFILE);
    }

    return (
        {   MODE        => \@MODE,
            NODES       => $NODES,
            NAMES       => $NAMES,
            TABLES      => \%TABLES,
            ORG         => $ORG,
			MAP         => $MAP,
            TAXID      => $TAXID,
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
    croak 'init_logging() needs VERBOSE parameter' unless @_ == 1;
    my ($VERBOSE) = @_;

    #create log file in same dir where script is running
    my $dir_out = path($0)->parent->absolute;    #removes perl script and takes absolute path from rest of path
    #say '$dir_out:', $dir_out;
    my ($app_name) = path($0)->basename =~ m{\A(.+)\.(?:.+)\z};   #takes name of the script and removes .pl or .pm or .t
    #say '$app_name:', $app_name;
    my $logfile = path( $dir_out, $app_name . '.log' )->canonpath;    #combines all of above with .log
    #say '$logfile:', $logfile;

=for Regex_debugging:
    # comment previous 3 lines when debugging regexes with Regexp::Debugger to disable this regex
	# and add this line instead
    my $logfile = 'collect_genomes_to_database.log'; 
	
=cut

    #colored output on windows
    my $osname = $^O;
    if ( $osname eq 'MSWin32' ) {
        require Win32::Console::ANSI;                                 #require needs import
        Win32::Console::ANSI->import();
    }

    #enable different levels based on VERBOSE flag
    my $log_level;
    foreach ($VERBOSE) {
        when (0) { $log_level = 'INFO'; }
        when (1) { $log_level = 'DEBUG'; }
        when (2) { $log_level = 'TRACE'; }
        default  { $log_level = 'INFO'; }
    }

    #levels:
    #TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    ###############################################################################
    #                              Log::Log4perl Conf                             #
    ###############################################################################
    # Configuration in a string ...
    my $conf = qq(
      log4perl.category.main              = $log_level, Logfile, Screen
     
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
# Usage      : create_db();
# Purpose    : creates database that will hold sequences to analyze, maps and others
# Returns    : nothing
# Parameters : ( $param_href ) -> params from command line
# Throws     : croaks if wrong number of parameters
# Comments   : first sub in chain, run only once at start (it drops database)
# See Also   :
sub create_db {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak ('create_db() needs a hash_ref' ) unless @_ == 1;
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
			#same as sth->execute( $_->{species_name}, $_->{ti}, $_->{assembly}, $_->{assembly_accession}, $_->{variation} );
			if ($@) {
				my $species_error = $_->{species_name};   #@col_loh is list of hashrefs therefore ->
				$log->error(qq|Report: insert failed for:$species_error (duplicate genome with PRE?)|);
				#say $@;
				next INSERT;
			}
			
			$log->info(qq|Report: inserted species:$_->{species_name} with taxid:$_->{ti}|);
		}


		my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensembl.org';
		my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );

		#part III: ftp download of PRE genomes
		my $REMOTE_DIR = 'pub/pre/fasta/pep';
		$param_href->{REMOTE_DIR} = $REMOTE_DIR;

        my $ftp_pre;
        $ftp_pre = Net::FTP::AutoReconnect->new( $REMOTE_HOST, Debug => 0 )
          or $log->logdie("Action: Can't connect to $REMOTE_HOST: $@");
        $ftp_pre->login( "anonymous", 'msestak@irb.hr' ) or $log->logdie( "Action: Can't login ", $ftp_pre->message );
        $ftp_pre->binary() or $log->logdie("Opening binary mode data connection failed for $_: $@");
        $ftp_pre->cwd($REMOTE_DIR) or $log->logdie( "Can't change working directory ", $ftp_pre->message );
        $ftp_pre->pasv() or $log->logdie("Opening passive mode data connection failed for $_: $@");
        $log->trace( "Report: location: ", $ftp_pre->pwd() );

		my @species_listing_pre = $ftp_pre->ls;
		#my @species_listing_pre = ('erinaceus_europaeus');
		$log->trace("@species_listing_pre");
        PRE:
        foreach my $species_dir_out (@species_listing_pre) {
            if ( $species_dir_out eq 'ancestral_alleles' ) {
                $log->trace("Action: ancestral_alleles skipped");
                next PRE;
            }

            #crucial to send $ftp_pre to the sub (else it uses old one from previous division)
            ftp_get_pre(
                { DIR => $species_dir_out, FTP => $ftp_pre, TABLE => $table_ensembl_end, DBH => $dbh, %{$param_href} } );
        }

        #part II: ftp download of vertebrate genomes
		$REMOTE_DIR  = 'pub/current_fasta/';
		$param_href->{REMOTE_DIR} = $REMOTE_DIR;

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
		$log->error( qq|Report: $species_dir not found in $table_info (not found in info files (probably deleted because of TAXID problems))| );
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
	    			$fasta_seq =~ s/\R//g;  #delete multiple newlines
					$fasta_seq = uc $fasta_seq;
					$fasta_seq =~ tr{*}{J};
					$fasta_seq =~ tr{A-Z}{}dc;
	  
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
# Usage      : ftp_get_pre(
#            : { DIR => $species_dir_out, FTP => $ftp, TABLE => $table_ensembl_end, DBH => $dbh, %{$param_href} } );
# Purpose    : downloads proteomes from Ensembl PRE only
# Returns    : nothing
# Parameters : hash_ref
# Throws     : 
# Comments   : it reconnects - usable for long downloads
# See Also   : calling sub (mode) ensembl_vertebrates()
sub ftp_get_pre {
	my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'ftp_get_pre() needs a $param_href' ) unless @_ == 1;

    my ($param_href) = @_;

    my $OUT         = $param_href->{OUT}   or $log->logcroak('no $OUT specified on command line!');
    my $species_dir = $param_href->{DIR}   or $log->logcroak('no $species_dir sent to ftp_get_pre!');
    my $ftp         = $param_href->{FTP}   or $log->logcroak('no $ftp sent to ftp_get_pre!');
    my $table_info  = $param_href->{TABLE} or $log->logcroak('no $table_info sent to ftp_get_pre!');
    my $dbh         = $param_href->{DBH}   or $log->logcroak('no $dbh sent to ftp_get_pre!');
    my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensembl.org';
    my $REMOTE_DIR  = $param_href->{REMOTE_DIR}  //= 'pub/current_fasta/';

	$log->trace("Action: working with $species_dir" );
	$log->trace("Report: location: ", $ftp->pwd() );
	
	my $spec_path = path($species_dir);
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
		$log->error( qq|Report: $species_dir not found in $table_info (not found in info files (probably deleted because of TAXID problems))| );
		$ftp->cdup();   #go one dir up to $REMOTE_DIR (cdup = current dir up)
		return;
	}

	#get fasta file inside
	my @pep_listing = $ftp->ls;
	FILE:
	foreach my $proteome (@pep_listing) {

		#skip all if nothing useful here
		if ($proteome =~ m/pre\.pep\.fa\z/) {
			#$ftp->cdup();
			$log->warn("Report: working with RAW fasta .prep.pep.fa file");
			#last FILE;
			get_raw_fasta( { SPEC_PATH => $spec_path, TAXID => $tax_id, PROTEOME => $proteome, FTP => $ftp, %{$param_href} } );
			next FILE;
		}

		#skip README and CHECKSUMS files
	    if ( ($proteome =~ m/README/) or ($proteome =~ m/CHECKSUMS/) or ($proteome =~ m/abinitio\.fa\.gz\z/) ) {
			$log->trace("Report: skipping $proteome");
			next FILE;
		}

		my $local_file;
	    if ($proteome =~ m/fa\.gz\z/) {
			$local_file = path($OUT, $proteome);
		}

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
            $log->trace("Action: STAT file:$stat_file already exists: appending");
        }
        open my $stat_fh, '>>', $stat_file or die "can't open file: $!";
		print {$stat_fh} path($REMOTE_HOST, $REMOTE_DIR, $spec_path, $proteome), "\t", $local_file, "\t";
		$ftp->cdup();   #go up one dir only to $REMOTE_DIR (cdup = current dir up)

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
					$fasta_seq = uc $fasta_seq;
					$fasta_seq =~ tr{*}{J};
					$fasta_seq =~ tr{A-Z}{}dc;
	  
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
		
		#skip all other files
		$log->warn("Report: skippping all other files");
		last FILE;   #hack for multiple files
	}   #foreach FILE end

	return;

}


### INTERNAL UTILITY ###
# Usage      : get_raw_fasta( { SPEC_PATH => $spec_path, TAXID => $tax_id, PROTEOME => $proteome, FTP => $ftp, %{$param_href} } );
# Purpose    : downloads proteomes from Ensembl PRE (only .fa files)
# Returns    : nothing
# Parameters : hash_ref
# Throws     : 
# Comments   : it reconnects - usable for long downloads
# See Also   : calling sub -> ftp_get_pre()
sub get_raw_fasta {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('get_raw_fasta() needs a $param_href') unless @_ == 1;

    my ($param_href) = @_;

    my $OUT = $param_href->{OUT} or $log->logcroak('no $OUT specified on command line!');
    my $ftp = $param_href->{FTP} or $log->logcroak('no $ftp sent to get_raw_fasta!');
    my $REMOTE_HOST = $param_href->{REMOTE_HOST} //= 'ftp.ensembl.org';
    my $REMOTE_DIR  = $param_href->{REMOTE_DIR}  //= 'pub/current_fasta/';
    my $proteome  = $param_href->{PROTEOME}  or $log->logcroak('no $proteome sent to get_raw_fasta!');
    my $spec_path = $param_href->{SPEC_PATH} or $log->logcroak('no $spec_path sent to get_raw_fasta!');
    my $tax_id    = $param_href->{TAXID}     or $log->logcroak('no $tax_id sent to get_raw_fasta!');

    my $local_file = path( $OUT, $proteome );

    #opens a filehandle to $OUT dir and downloads file there
    #Net::FTP get(REMOTE_FILE, LOCAL_FILE) accepts filehandle as LOCAL_FILE.
    open my $local_fh, ">", $local_file or $log->logdie("Can't write to $local_file:$!");
    $ftp->get( $proteome, $local_fh ) and $log->info("Action: download to $local_file");

    #print stats and go up 2 dirs
    my $stat_file = path($OUT)->parent;
    $stat_file = path( $stat_file, 'statistics_ensembl_all.txt' )->canonpath;
    if ( -f $stat_file ) {
        $log->trace("Action: STAT file:$stat_file already exists: appending");
    }
    open my $stat_fh, '>>', $stat_file or die "can't open file: $!";
    print {$stat_fh} path( $REMOTE_HOST, $REMOTE_DIR, $spec_path, $proteome ), "\t", $local_file, "\t";
    $ftp->cdup();    #go up one dir only to $REMOTE_DIR (cdup = current dir up)

    #BLOCK for writing proteomes to taxid file
    {
        open my $downloaded_fh, "<", $local_file or $log->logdie("Can't open $local_file: $!");
        my $path_taxid = path( $OUT, $tax_id );
        open my $genome_ti_fh, ">", $path_taxid or $log->logdie("Can't write $tax_id: $!");

        #return $/ value to newline for $header_first_line
        local $/ = "\n";
        my $header_first_line = <$downloaded_fh>;
        print {$stat_fh} $header_first_line, "\t";

        #return to start of file
        seek $downloaded_fh, 0, 0;

        #look in larger chunks between records
        local $/ = ">";
        my $line_count = 0;
        while (<$downloaded_fh>) {
            chomp;
            $line_count++;

            if (m/\A([^\h]+)(?:\h+)*(?:[^\v]+)*\v(.+)/s) {

                my $header = $1;

                my $fasta_seq = $2;
                $fasta_seq =~ s/\R//g;       #delete multiple newlines (also forgets %+ hash)
                $fasta_seq = uc $fasta_seq;
                $fasta_seq =~ tr{*}{J};      #replace * with J for cd-hit
                $fasta_seq =~ tr{A-Z}{}dc;   #replace all chars that are not in A-Z range

                print $genome_ti_fh ( '>', $header, "\n", $fasta_seq, "\n" );
            }
        }    #end while

        if ($line_count) {
            $line_count--;    #it has one line to much
            $log->debug(qq|Action: saved to $path_taxid with $line_count lines|);
            print {$stat_fh} $line_count, "\n";
        }

		#unlink downloaded fasta file
		if (-f $local_file) {
			unlink $local_file and $log->error("Action: fasta file:$local_file unlinked");
		}
    }    #block writing proteomes to taxid end

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
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
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
	core_db VARCHAR(200) NOT NULL,
	species_id INT UNSIGNED NOT NULL,
	invis VARCHAR(10),
    PRIMARY KEY(ti, species),
	KEY(species),
	KEY(id)
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
			(species_name, species, division, ti, assembly, assembly_accession, genebuild, variation, pan_compara, peptide_compara, genome_alignments, other_alignments, core_db, species_id, invis)
    		};
    		eval { $dbh->do($load_info, { async => 1 } ) };
			my $rows_info = $dbh->mysql_async_result;
    		$log->debug( "Action: $table_info loaded with $rows_info rows!" ) unless $@;
    		$log->debug( "Action: loading $table_info failed: $@" ) if $@;

			#DELETE genomes with same tax_ids
			my $delete_dup = qq{
			DELETE ens FROM $table_info AS ens
			INNER JOIN $table_info AS ens2 ON ens.ti = ens2.ti
			WHERE ens.id > ens2.id};
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
			    $fasta =~ s/\R//g;      #delete multiple newlines (all vertical and horizontal space)
				$fasta = uc $fasta;     #uppercase fasta
			    $fasta =~ tr{*}{J};     #replace *(stop codon) with J (not used in BLOSSUM62) for cd-hit (return after cd-hit)
			    $fasta =~ tr{A-Z}{}dc;  #delete all special characters (all not in A-Z)
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
		create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );
		$log->trace("Report: $create_query");

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
		$log->trace("Report: $create_query");

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
    my $TAXID = $param_href->{TAXID} or $log->logcroak( 'no $TAXID specified on command line!' );

    #Using TAXID to get unique table names
    my $table    = "tree$TAXID";
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
    my $TAXID = $param_href->{TAXID} or $log->logcroak('no $TAXID file specified on command line!');
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
    my $table           = "tree_fn_ret_ph$TAXID";
    my $table_phylogeny = "retrieve_phylogeny$TAXID";
    my $function        = "fn_retrieve_phylogeny$TAXID";

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
    my $TAXID = $param_href->{TAXID} or $log->logcroak('no $TAXID file specified on command line!');

    #using $$ as process_id to get unique table_name
    my $table_ret       = "tree_fn_ret_ph$TAXID";
    my $table_phylogeny = "retrieve_phylogeny$TAXID";
    my $function        = "fn_retrieve_phylogeny$TAXID";

    #test function on tax_id from command line
    my $test_fn_retrieve_query = qq{
    SELECT $function(?)
    };
    my $sth = $dbh->prepare($test_fn_retrieve_query);
    eval { $sth->execute($TAXID); };

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
    my $TAXID = $param_href->{TAXID} or $log->logcroak('no $TAXID file specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';

    #using process_id $$ to get unique table_name
    my $table_phylo     = "phylo_${TAXID}";
    my $procedure       = "proc_create_${table_phylo}";    #phylo table has $TAXID
    my $table_tree      = "tree$TAXID";
    my $table_phylogeny = "retrieve_phylogeny$TAXID";

    #drop unique proc_create_phylo$TAXID
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
# Parameters : ( $param_href, $TAXID)
# Throws     : croaks for parameters
# Comments   : needs $TAXID from command line
# See Also   :
sub call_proc_phylo {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'call_proc_phylo() needs a hash_ref' ) unless @_ == 1;
    my ($param_href) = @_;

    my $dbh    = dbi_connect($param_href);
    my $TAXID = $param_href->{TAXID} or $log->logcroak( 'no $TAXID specified on command line!' );
    my $NODES  = $param_href->{NODES}  or $log->logcroak( 'no $NODES specified on command line!' );

    #using $TAXID to get unique proc name (when running in parallel with create subs)
    #using proc name from command line when running solo
    my $proc = defined $param_href->{PROC} ? $param_href->{PROC} : "proc_create_phylo_$TAXID";
    $log->trace("Report: using procedure $proc!");
    my $table_phylo = "phylo_$TAXID";
    my $table_tree  = "tree$TAXID";
    my $table_ret   = "tree_fn_ret_ph$TAXID";

	#throws error: Commands out of sync; you can't run this command now
	#without changes to $dbh
	#$dbh->{mysql_use_result} = 1;
	$dbh->{mysql_server_prepare} = 0;   #procedures don't work with server side prepare
	my $dbh_trace = sprintf(Dumper tied %$dbh);
	$log->trace("$dbh_trace");

    #CALL proc_create_phylo$TAXID
    my $call_proc_query = qq{
    CALL $proc($TAXID);
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
        $log->error("Action: printing STDERR:$stderr_dry");
        $log->info("Action: database $DATABASE schema backup succeeded at $target_dry");
    }
    else {
        $log->error("Action: printing STDERR:$stderr_dry");
        $log->error("Action: database $DATABASE schema backup FAILED at $target_dry");
    }

    my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
    if ( $exit == 0 ) {
        $log->error("Action: printing STDERR:$stderr");
        $log->info("Action: database $DATABASE table backup succeeded at $target");
    }
    else {
        $log->error("Action: printing STDERR:$stderr");
        $log->error("Action: database $DATABASE table backup FAILED at $target");
    }

    return;
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

    my $DATABASE     = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES       = %{ $param_href->{TABLES} };
    my $NR_CNT_TBL   = $TABLES{nr_cnt};
    my $TI_FILES_TBL = $TABLES{ensembl_genomes};

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
    $log->debug( "Action: table $NR_CNT_TBL deleted $rows_del rows from Ensembl" ) unless $@;
    $log->error( "Action: deleting $NR_CNT_TBL failed: $@" ) if $@;

	#DELETE genomes smaller than 2000 proteins
	my $delete_cnt2 = qq{
	DELETE nr FROM $NR_CNT_TBL AS nr
	WHERE genes_cnt <= 2000
    };
    eval { $dbh->do($delete_cnt2, { async => 1 } ) };
	my $rows_del2 = $dbh->mysql_async_result;
    $log->debug( "Action: table $NR_CNT_TBL deleted $rows_del2 rows smaller than 2000 genes" ) unless $@;
    $log->error( "Action: deleting $NR_CNT_TBL failed: $@" ) if $@;

	#delete species if it contains group in name
    my $species_query = qq{
    SELECT species_name
    FROM $NR_CNT_TBL
    ORDER BY species_name
    };
    my @species_names = map { $_->[0] } @{ $dbh->selectall_arrayref($species_query) };

	my @groups = grep { /group/ } @species_names;
	#say join ("\n", @groups);
	my $del_grp = qq{
	DELETE nr FROM $NR_CNT_TBL AS nr
	WHERE species_name = ?
	};
	my $sth_del = $dbh->prepare($del_grp);
	my $grp_del = 0;
	foreach my $species_name (@groups) {
		eval { $sth_del->execute($species_name); };
		$grp_del++;
		$log->debug("Action: deleted group:$species_name from table:$NR_CNT_TBL") unless $@;
		$log->error("Action: failed delete for group:$species_name for table:$NR_CNT_TBL") if $@;
	}
	$log->info("Action: deleted $grp_del groups from table:$NR_CNT_TBL");


	my $q = qq{SELECT COUNT(*) FROM $NR_CNT_TBL WHERE genes_cnt >= ?};
	my $sth = $dbh->prepare($q);
    foreach my $i (qw/2000 3000 4000 5000 6000 7000 8000 9000 10000 15000 20000 25000 30000/) {
		$sth->execute($i);
		my $genome_cnt = $sth->fetchrow_array();
		$log->info("Report: found $genome_cnt genomes larger than $i proteins in table:$NR_CNT_TBL");
	}
	
	$sth->finish;
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
	#Viruses deleted in nodes_raw import 
	#unclassified deleted in MakeTree
	#only some other sequences left
		#12884 => 'Viroids',
		#10239 => 'Viruses',
		#12908 => 'unclassified sequences',
    my %tis_to_del = (
        28384 => 'other sequences',
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

	#get count of genus if there is more than 1 (into hash)
	my $found_genus_species = '';
	my %found_hash_cnt;
	foreach my $species (@species_names) {
		(my $genus_species = $species) =~ s/\A([^_]+)_(.+)_(?:.+)\z/$1_$2/g;

		if ($genus_species eq $found_genus_species) {
			$found_hash_cnt{$genus_species}++;   #add to hash only if found earlier (only duplicates)
		}
		$found_genus_species = $genus_species;   #now found has previous genus_species
	}
	#print Dumper(\%found_hash_cnt);

	#now get found species into hash of arefs to delete them later
	my (%hoarefs);
	foreach my $species2 (@species_names) {
		(my $genus_spec = $species2) =~ s/\A([^_]+)_(.+)_(?:.+)\z/$1_$2/g;

		if (exists $found_hash_cnt{$genus_spec}) {
			push @{ $hoarefs{$genus_spec} }, $species2;   #created Hash of array refs
		}
	}
	#print Dumper(\%hoarefs);

	#search for species to delete
	while (my ($spec_key, $grp_aref) = each %hoarefs) {
		my @spec_group = @{ $grp_aref };   #full group here
		my %hash_to_print = ($spec_key => $grp_aref);
		foreach my $spec (@{ $grp_aref }) {
			my @spec_search_group = map { /\A$spec\z/ ? () : $_} @spec_group;            #remove only species that is exactly the same
			@spec_search_group = map { /\A$spec(\d+)\z/ ? () : $_} @spec_search_group;   #remove species that is different only in ending number too
			@spec_search_group = map { /\A$spec([^_]+)\z/ ? () : $_} @spec_search_group; #remove species that is different only in ending letters after _
			if ( grep { /$spec/ } @spec_search_group) {                       #search for that species among the other species (anywhere in name)
				#$log->trace("Report: found match for:{$spec} in:{@spec_search_group}");
				print Dumper(\%hash_to_print);     #print group before delete
				$log->trace( "Action: deleting:$spec" );

				#DELETE species from nr_base_eu_cnt table (because it is species with strains present)
				my $delete_spec = qq{
				DELETE nr FROM $NR_CNT_TBL AS nr
				WHERE species_name = ('$spec');
			    };
			    eval { $dbh->do($delete_spec, { async => 1 } ) };
				my $rows_del_spec = $dbh->mysql_async_result;
			    $log->debug( "Action: table $NR_CNT_TBL deleted $rows_del_spec rows for:{$spec}" ) unless $@;
			    $log->debug( "Action: deleting $NR_CNT_TBL failed for:$spec: $@" ) if $@;
			}
			else {
				#$log->trace("Report: no match for:$spec in {@spec_search_group}");
			}
		}
	}

	#check if genomes larger than 30_000 seq and offer to delete from nr count table
    my $sel_large = qq{
	SELECT ti, genes_cnt, species_name
	FROM $NR_CNT_TBL
	WHERE genes_cnt >= 30000
	};
	my %ti_large = map { $_->[0], [ $_->[1], $_->[2] ] } @{ $dbh->selectall_arrayref($sel_large) };
    my $cnt_pairs = keys %ti_large;
    $log->info("Report: Found $cnt_pairs ti->[genes_cnt-species_name] pairs");

	#prepare delete query for large genomes
	my $del_q = qq{
	DELETE nr FROM $NR_CNT_TBL AS nr
	WHERE ti = ?
	};
	my $sth_del = $dbh->prepare($del_q);

    while ( my ( $ti, $species_ref ) = each %ti_large ) {
        my $genes_cnt    = $species_ref->[0];
        my $species_name = $species_ref->[1];
		my $decision = prompt "Do you want to delete species:{$species_name} with->$genes_cnt genes from:NCBI?",
		               -yn,
					   -single;
		if ($decision eq 'y') {
			eval {$sth_del->execute($ti); };
			$log->error("Action: delete failed for species:$species_name") if $@;
			$log->debug("Action: deleted species:{$species_name} from NCBI with $genes_cnt from $NR_CNT_TBL") unless $@;
		}
        else {
            $log->trace("Action: species:{$species_name} from NCBI with $genes_cnt left in $NR_CNT_TBL");
        }
    }

	#report the changes made
	my $q = qq{SELECT COUNT(*) FROM $NR_CNT_TBL WHERE genes_cnt >= ?};
	my $sth = $dbh->prepare($q);
    foreach my $i (qw/2000 3000 4000 5000 6000 7000 8000 9000 10000 15000 20000 25000 30000/) {
		$sth->execute($i);
		my $genome_cnt = $sth->fetchrow_array();
		$log->info("Report: found $genome_cnt genomes larger than $i proteins in table:$NR_CNT_TBL");
	}
	
	$sth_del->finish;
	$sth->finish;
	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : del_total_genomes( $param_href );
# Purpose    : deletes TOTAL species genomes that have subspecies or strain genomes (second step)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : first it creates ti_full_list table to hold all genomes
#            : deletes hybrid genomes
#            : it deletes genomes that have species and strain genomes in full dataset
#            : (both nr and existing genomes)
# See Also   : del_nr_genomes() - first step
sub del_total_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_total_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $DATABASE = $param_href->{DATABASE}    or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_CNT_TBL   = $TABLES{nr_cnt};
    my $TI_FILES_TBL = $TABLES{ensembl_genomes};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info( "---------->JOIN-ing two tables: $NR_CNT_TBL and $TI_FILES_TBL" );

    #create table
	my $table_list = "ti_full_list";
    my $create_query = qq{
    CREATE TABLE $table_list (
    ti INT UNSIGNED NOT NULL,
    genes_cnt INT UNSIGNED NULL,
	species_name VARCHAR(200) NULL,
	source VARCHAR(20) NULL,
    PRIMARY KEY(ti),
	KEY(genes_cnt),
	KEY(species_name)
    )ENGINE=$ENGINE CHARSET=ascii
    };
	create_table( { TABLE_NAME => $table_list, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

	#insert NR genomes
    my $insert_nr = qq{
    INSERT INTO $table_list (ti, genes_cnt, species_name, source)
    SELECT ti, genes_cnt, species_name, 'NCBI'
    FROM $NR_CNT_TBL
	ORDER BY ti
    };
    eval { $dbh->do($insert_nr, { async => 1 } ) };
    my $rows = $dbh->mysql_async_result;
    $log->debug( "Action: import inserted $rows rows!" ) unless $@;
    $log->error( "Action: loading $table_list failed: $@" ) if $@;

	#insert Ensembl genomes
    my $insert_ti = qq{
    INSERT INTO $table_list (ti, genes_cnt, species_name, source)
    SELECT ti, genes_cnt, species_name, source
	FROM $TI_FILES_TBL
	ORDER BY ti
    };
    eval { $dbh->do($insert_ti, { async => 1 } ) };
    my $rows2 = $dbh->mysql_async_result;
    $log->debug( "Action: import inserted $rows2 rows!" ) unless $@;
    $log->error( "Action: loading $table_list failed: $@" ) if $@;

	#delete species that are hybrids
	my $del_hybrid = sprintf( q{
	DELETE ti FROM %s AS ti
	WHERE species_name LIKE %s }, $dbh->quote_identifier($table_list), $dbh->quote('%\_x\_%') );
    #say $del_hybrid;
	eval { $dbh->do($del_hybrid, { async => 1 } ) };
	my $rows_hy = $dbh->mysql_async_result;
	$log->debug( "Action: deleted $rows_hy hybrid species from $table_list") unless $@;
	$log->error( "Action: table $table_list delete for hybrids failed: $@" ) if $@;

	#report the changes made
	my $genome_cnt = $dbh->selectrow_array("SELECT COUNT(*) FROM $table_list");
	$log->info("Report: found $genome_cnt genomes in table:$table_list");
	
	$dbh->disconnect;

	return;
}



### INTERFACE SUB ###
# Usage      : import_raw_names( $param_href );
# Purpose    : loads raw names file (names.dmp) to MySQL database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it tries to emulate Robert's work and format (tilda in 3rd column and underscores everywhere)
#            : uses bulk insert (23k rows/s)
# See Also   :
sub import_raw_names {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_raw_names() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $INFILE   = $param_href->{INFILE}   or $log->logcroak('no $INFILE specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $table = path($INFILE)->basename;
    $table =~ tr/./_/;    #for files that have dots in name

    #get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info("Report:---------->Importing names:$table");

    my $create_query = sprintf(
        qq{
	CREATE TABLE %s (
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
	ti INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NOT NULL,
	species_synonym VARCHAR(100),
	name_type VARCHAR(100),
	PRIMARY KEY(id, ti),
	KEY(ti),
	KEY(species_name)
	)ENGINE=$ENGINE CHARACTER SET=ascii }, $dbh->quote_identifier($table)
    );
    create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );
    $log->trace("Report: $create_query");

    #need to change format of names.dmp (print it and load it to MySQL)
    #get name with date
    my $now       = DateTime::Tiny->now;
    my $date      = $now->year . '_' . $now->month . '_' . $now->day;
    my $names_out = 'names_raw_' . $date;
    $names_out = path( $OUT, $names_out );

    open my $names_fh,     "<:encoding(ASCII)", $INFILE    or $log->logdie("Error: can't open $INFILE:$!");
    open my $names_out_fh, ">:encoding(ASCII)", $names_out or $log->logdie("Error: can't write to $names_out:$!");

    {
        #prepare SQL for insert
        my @columns             = qw/ti species_name species_synonym name_type/;
        my $columnlist          = join( ", ", @columns );

        my $insert = qq{
			INSERT INTO $table ($columnlist) VALUES 
			};
		my $query      = $insert;
		my $count      = 0;      #count to insert
		my $max_rows   = 10000;  #adjust it to your needs
		my $inserted   = 0;      #count to report

        #reading part
        local $/ = "\t\|\n";
      NAMES:
        while (<$names_fh>) {
            chomp;

            #select what to use
            my ( $ti, $species_name, $species_synonym, $name_type )
              = split( /\t\|\t/, $_ );    #$_ is default for split and first argument to split is regex//
            $species_synonym = '~' if $species_synonym eq '';    #third column usually empty
			#next NAMES if $name_type ne 'scientific name';       #ignore all other names

            #format what you selected (Robert's underscores)
            $species_name    =~ tr/0-9A-Za-z_//dc;
            $species_synonym =~ tr/0-9A-Za-z_//dc;
            $name_type       =~ tr/0-9A-Za-z_//dc;

            print {$names_out_fh} "$ti\t$species_name\t$species_synonym\t$name_type\n";

			#start the insert
			my @values = ("$ti", "$species_name", "$species_synonym", "$name_type");
			$query .= "," if $count++;
		    $query .= "("  . join(",", map { $dbh->quote($_) } @values ) . ")";

		    if ($count == $max_rows) {
		        $dbh->do($query) or die "something wrong ($DBI::errstr)";
		        $query = $insert;   #reset to base query
				$inserted += $count;
				$log->trace("Action: imported $count rows");
		        $count = 0;         #reset to base count
		    }
        }
		#run at end
		$dbh->do($query) if $count;   #insert remaining rows
		$inserted += $count;
		$log->trace("Action: imported $count rows");
		$log->info("Action: inserted $inserted rows to names:$table");

    }   #end block and end of local $/

    return;
}

### INTERFACE SUB ###
# Usage      : import_raw_nodes( $param_href );
# Purpose    : loads raw nodes.dmp from NCBI to MySQL
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it tries to emulate Robert's work and format
#            : it removes Phages, viruses, Synthetic and Environmental samples
#            : uses bulk insert with 10000 rows at once
# See Also   :
sub import_raw_nodes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('import_raw_nodes() needs a hash_ref') unless @_ == 1;
    my ($param_href) = @_;

    my $INFILE   = $param_href->{INFILE}   or $log->logcroak('no $INFILE specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $table = path($INFILE)->basename;
    $table =~ tr/./_/;    #for files that have dots in name

    #get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info("---------->Importing nodes $table");
    my $create_query = sprintf(
        qq{
    CREATE TABLE IF NOT EXISTS %s (
    ti INT UNSIGNED NOT NULL,
    parent_ti INT UNSIGNED NOT NULL,
	division_id TINYINT UNSIGNED NOT NULL,
    PRIMARY KEY(ti),
    KEY(parent_ti),
	KEY(division_id)
    )ENGINE=$ENGINE CHARACTER SET=ascii }, $dbh->quote_identifier($table)
    );
    create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

    #need to change format of nodes.dmp (print it and load it to MySQL)
    #get name with date
    my $now       = DateTime::Tiny->now;
    my $date      = $now->year . '_' . $now->month . '_' . $now->day;
    my $nodes_out = 'nodes_raw_' . $date;
    $nodes_out = path( $OUT, $nodes_out );

    open my $nodes_fh,     "<:encoding(ASCII)", $INFILE    or $log->logdie("Error: can't open $INFILE:$!");
    open my $nodes_out_fh, ">:encoding(ASCII)", $nodes_out or $log->logdie("Error: can't write to $nodes_out:$!");

    {
        #prepare SQL for insert
		my $base_query = qq{INSERT INTO $table VALUES };
		my $query      = $base_query;
		my $count      = 0;      #count to insert
		my $max_rows   = 10000; # adjust it to your needs
		my $inserted   = 0;      #count to report

        #reading part
        local $/ = "\t\|\n";
      NODES:
        while (<$nodes_fh>) {
            chomp;

            #select what to use
            my ( $ti, $parent_ti, undef, undef, $division_id, undef, undef, undef, undef, undef, undef, undef, undef )
              = split( /\t\|\t/, $_ );    #$_ is default for split and first argument to split is regex//

			#root problems
			if (($ti == 1) and ($parent_ti == 1)) {
				$log->warn("Action: root excluded from file and table: $table!");
				#print {$nodes_out_fh} "$ti\t$parent_ti\n";
				#my $false_parent = 100_000_000;
				#$sth->execute( $ti, $false_parent, $division_id );
				next NODES;
			}

			#skip divisions:Viruses, synthetic, environmental
			foreach ($division_id) {
				when (3)  {next NODES;}   #Phages     say "Phyges out: $division_id"; 
				when (7)  {next NODES;}   #Synthetic  say "Synthe out: $division_id"; 
				when (9)  {next NODES;}   #Viruses    say "Virus  out: $division_id"; 
				when (11) {next NODES;}   #Environmental samples say "Enviro out: $division_id";
			}
            print {$nodes_out_fh} "$ti\t$parent_ti\n";

			#start of insert
			my @values = ("$ti", "$parent_ti", "$division_id");
			$query .= "," if $count++;
		    $query .= "("  . join(",", map { $dbh->quote($_) } @values ) . ")";

		    if ($count == $max_rows) {
		        $dbh->do($query) or die "something wrong ($DBI::errstr)";
		        $query = $base_query;
				$inserted += $count;
				$log->trace("Action: imported $count rows");
		        $count = 0;
		    }
        }
		#run on end
		$dbh->do($query) if $count;   #insert remaining rows
		$inserted += $count;
		$log->trace("Action: imported $count rows");
		$log->info("Action: inserted $inserted rows to nodes:$table");

    }    #end block and end of local $/

    return;
}


### INTERFACE SUB ###
# Usage      : print_nr_genomes( $param_href );
# Purpose    : prints nr genomes to dropdox/D.../db../data/eukarya
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it extracts genomes from database and prints them to $OUT
# See Also   : 
sub print_nr_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'print_nr_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );
    my $DATABASE = $param_href->{DATABASE}    or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_TI_FASTA = $TABLES{nr_ti_fasta};
    my $TI_FULLLIST = $TABLES{ti_full_list};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#get all tax_ids that belong to nr and print them to $OUT
    my $tis_query = qq{
    SELECT ti
    FROM $TI_FULLLIST
	WHERE source = 'NCBI'
    ORDER BY ti
    };
    my @tis = map { $_->[0] } @{ $dbh->selectall_arrayref($tis_query) };

	#starting iteration over @tis to extract genomes from nr_ti_gi_fasta and  print to $OUT
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
		FROM $NR_TI_FASTA
		WHERE ti = $ti
		ORDER BY gi;
		};

		eval { $dbh->do($genome_query) };
		$log->error( "Action: file $genome_out failed to print: $@" ) if $@;
		$log->debug( "Action: file $genome_out printed" ) unless $@;
	
	}

	my $nr_genomes = @tis;
	$log->info("Report: printed $nr_genomes nr genomes to $OUT");
	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : make_db_dirs( $param_href );
# Purpose    : creates directories to store files for creation of new database
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
#            : logs all creation
# Comments   : it needs starting dir
#            : first time it creates dirs
#            : second time it leaves existing dirs and only creates new ones
#            : optional -if parameter is update_phylogeny file
# See Also   : 
sub make_db_dirs {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('make_db_dirs() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    #my $IN     = $param_href->{IN}       or $log->logcroak('no $IN specified on command line!');
    my $INFILE = $param_href->{INFILE};                                                     #optional
    my $OUT    = $param_href->{OUT} or $log->logcroak('no $OUT specified on command line!');

    #get new handle

    my @dirs = qw{
      data/ensembl_ftp
      data/ensembl_vertebrates
      data/ensembl_all
      data/nr_raw
      data/nr_genomes
      data/jgi
      data/jgi_clean
      data/xml
      data/external
      data/all_raw
      data/all_sync
      data/all_ff_final
      data/cdhit
      doc
      src
	  };

	$log->trace("All dirs:@dirs");

    foreach my $dir (@dirs) {
        my $path_dir = path( $OUT, $dir )->canonpath;
        if ( -d $path_dir ) {
            $log->warn("Report: $path_dir already exists. Not overwritten.");
        }
        else {
            path($path_dir)->mkpath( { chmod => 0777 } ) and $log->info("Action: created $path_dir");
        }
    }

    #used to copy update_phylogeny7.tsv
    if ($INFILE) {
        my $doc_path = path( $OUT, 'doc' )->canonpath;
        my $outfile = path($INFILE)->basename;
        $outfile = path( $OUT, 'doc', $outfile )->canonpath;

        if ( -f $outfile ) {
            $log->warn("Report: $outfile already exists");
        }
        else {
            path($INFILE)->copy($doc_path)
              and $log->debug("Action: file $INFILE copied to:$outfile");
        }
    }

    return;
}


### INTERFACE SUB ###
# Usage      : del_missing_ti( $param_href );
# Purpose    : deletes all taxids not found in nodes_after_MakeTree
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : first it collects all tis not found in nodes_cleaned
#            : then it deletes them from gi_taxid_prot (or nr_ti_gi_fasta) table
# See Also   : 
sub del_missing_ti {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_missing_ti() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $NR_TBL   = $TABLES{nr};
    my $NODES    = $TABLES{nodes};
    my $NAMES    = $TABLES{names};

    #get new handle
    my $dbh = dbi_connect($param_href);

	#part I. get tis to delete
	my $get_tis_q = qq{
		SELECT DISTINCT gi.ti
		FROM $NR_TBL AS gi
		LEFT JOIN $NODES AS nod ON gi.ti = nod.ti
		WHERE nod.ti IS NULL
	};
	my @tis_to_del = map{ $_->[0] } @{ $dbh->selectall_arrayref($get_tis_q) };
	my $num_ti_found = @tis_to_del;
	$log->info("Report: Found $num_ti_found missing taxids");

	#delete each ti in gi_taxid_prot table
	my $del_ti = qq{
		DELETE gi FROM $NR_TBL AS gi
		WHERE ti = ?
		};
	my $sth = $dbh->prepare($del_ti);

	my $cnt_del = 0;
	foreach my $ti (@tis_to_del) {
	
		eval { $sth->execute($ti) };
	
		my $rows_del = $sth->rows;
		$cnt_del += $rows_del;
		$log->debug( "Action: table $NR_TBL deleted $rows_del rows for ti:$ti" ) unless $@;
		$log->error( "Action: deleting $NR_TBL failed for ti:$ti $@" ) if $@;
	}

	$log->info("Report: deleted total of $cnt_del rows in mode: missing");

	#delete all names with single part (genus, division, ...) and keep species names like Homo_sapiens
	#first get all species_names
	my $species_q = qq{
	SELECT DISTINCT gi.ti, na.species_name
	FROM $NR_TBL AS gi
	INNER JOIN $NAMES AS na ON gi.ti = na.ti
    };
    my %ti_species = map { $_->[0], $_->[1] } @{ $dbh->selectall_arrayref($species_q) };
	my $cnt_species_pairs = keys %ti_species;
	$log->info("Report: Found $cnt_species_pairs ti-species_name pairs");

	my $cnt_sp_del = 0;
	SPECIES:
	while (my ($ti, $species) = each %ti_species) {
		if (! defined $species) {
			$log->warn("Report: NULL species_name found for ti:$ti");
			next SPECIES;
		}
		if ($species =~ m{\A(?:[^_]+)_(?:.+)\z}g) {
			#$log->trace("Report: species:$species skipped for ti:$ti");
			next SPECIES;
		}
		else {
	    	eval { $sth->execute($ti) };

			my $rows_sp = $sth->rows;
			$cnt_sp_del += $rows_sp;
			$log->debug( "Action: table $NR_TBL deleted $rows_sp rows for species:$species with ti:$ti" ) unless $@;
			$log->error( "Action: deleting $NR_TBL failed for ti:$ti $@" ) if $@;
		}
	}

	$log->info("Report: deleted total of $cnt_sp_del rows in mode: genera");

	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : copy_external_genomes( $param_href );
# Purpose    : prints genomes outside Ensembl or  present genomes to dropdox/D.../db../data/eukarya
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it takes genomes from old directory (base) and prints them to $OUT
# See Also   : 
sub copy_external_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'copy_external_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );
	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );
	my $IN       = $param_href->{IN}   or $log->logcroak( 'no $IN specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $TI_FULLLIST = $TABLES{ti_full_list};
    my $NAMES       = $TABLES{names};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#get all existing genomes from $IN
	my @ti_files = File::Find::Rule->file()
								   ->name(qr/\A\d+\z/)
								   ->in($IN);
	@ti_files = sort {$a cmp $b} @ti_files;
	my @external_tis = map {path($_)->basename} @ti_files;
	#print Dumper(\@external_tis);

	#FIRST compare to ensembl_all and nr_genomes and jgi_clean directories
	my $ens_path = path(path($OUT)->parent, 'ensembl_all')->canonpath;
	my $nr_path  = path(path($OUT)->parent, 'nr_genomes')->canonpath;
	my $jgi_path  = path(path($OUT)->parent, 'jgi_clean')->canonpath;

    my @ti_files_ens = File::Find::Rule->file()->name(qr/\A\d+\z/)->in($ens_path);
    #print Dumper(\@ti_files_ens);
    my %ens_tis = map { path($_)->basename => undef } @ti_files_ens;
    #print Dumper(\%ens_tis);

    my @ti_files_nr = File::Find::Rule->file()->name(qr/\A\d+\z/)->in($nr_path);
    my %nr_tis = map { path($_)->basename => undef } @ti_files_nr;

    my @ti_files_jgi = File::Find::Rule->file()->name(qr/\A\d+\z/)->in($jgi_path);
    my %jgi_tis = map { path($_)->basename => undef } @ti_files_jgi;

	my @not_found;
	foreach my $ti (@external_tis) {
		my $ti_orig_loc = path($IN, $ti)->canonpath;
		if ( (! exists $ens_tis{$ti}) and (! exists $nr_tis{$ti}) and (! exists $jgi_tis{$ti})) {
			push @not_found, $ti_orig_loc;
		}
		else {
			$log->warn("Action: $ti_orig_loc excluded because found in ensembl_all or nr_genomes or jgi_clean");
		}
	}
	#print Dumper(\@not_found);

	#SECOND check for existence in TI_FULLLIST and copy to external
	#insert query to insert to ti_full_list
	my $q_in = qq{
	INSERT INTO ti_full_list (ti, genes_cnt, source)
	VALUES (?, ?, 'external')
	};
	my $sth = $dbh->prepare($q_in);

	#select query to get ti for comparison to fasta_file
	my $q_sel = qq{
	SELECT ti FROM ti_full_list
	WHERE ti = ?
	};
	my $sth_sel = $dbh->prepare($q_sel);

	#starting iteration over @ti_files to copy genomes from $IN to $OUT
	foreach my $ti_file (@not_found) {
		$log->trace( "Action: working on $ti_file" );
		my $ti_from_file = path($ti_file)->basename;
		my $end_ti_file = path($OUT, $ti_from_file);
		
		#compare ti_from_file to ti in db to see if it exists
		$sth_sel->execute($ti_from_file);
		my ($ti_from_db) = $sth_sel->fetchrow_array();

		if ($ti_from_db) {
			$log->error("Ti file:$ti_file found in TI_FULLLIST");
			#delete ti_files (genomes) if they exist in $OUT dir and TABLE
			if (-f $end_ti_file) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked" );
			}
		}
		else {
			#unlink if already there
			if (-f $end_ti_file) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked" );
			}
			#copy fasta from $IN to $OUT and transform as needed
			$log->info( "Action: file $ti_file not found in modified list:$TI_FULLLIST" );
			my $fasta_cnt = collect_fasta_print({FILE => $ti_file, TAXID => $ti_from_file, %{$param_href} });

			#insert into ti_full_list
			eval {$sth->execute($ti_from_file, $fasta_cnt); };
			my $rows = $sth->rows;
			$log->debug( "Action: table $TI_FULLLIST inserted $rows rows for ti:$ti_from_file" ) unless $@;
			$log->error( "Action: inserting $TI_FULLLIST failed for ti:$ti_from_file:$@" ) if $@;
			if ($@) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked because it already exists in $TI_FULLLIST table" );
			}
		}
	}
	
	#UPDATE with species_names
    my $up_sp = qq{
	UPDATE $TI_FULLLIST AS ti
	SET ti.species_name = (SELECT DISTINCT na.species_name
	FROM $NAMES AS na WHERE ti.ti = na.ti)
	WHERE ti.species_name IS NULL;
    };
    eval { $dbh->do($up_sp, { async => 1 } ) };
	my $rows_up = $dbh->mysql_async_result;
    $log->debug( "Action: update to $TI_FULLLIST updated $rows_up rows for external" ) unless $@;
    $log->error( "Action: updating $TI_FULLLIST failed: $@" ) if $@;

	#count genomes coming from external
	my $ext_cnt = $dbh->selectrow_array("SELECT COUNT(*) FROM $TI_FULLLIST WHERE source = 'external'");
	$log->info("Report: found $ext_cnt external genomes in table:$TI_FULLLIST");

	$sth->finish;
	$sth_sel->finish;
	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : copy_jgi_genomes( $param_href );
# Purpose    : prints genomes outside Ensembl or  present genomes to dropdox/D.../db../data/eukarya
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it takes genomes from old directory (base) and prints them to $OUT
# See Also   : 
sub copy_jgi_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'copy_jgi_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );
	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );
	my $IN       = $param_href->{IN}   or $log->logcroak( 'no $IN specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $TI_FULLLIST = $TABLES{ti_full_list};
    my $NAMES       = $TABLES{names};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#get all existing genomes from $IN
	my @ti_files = File::Find::Rule->file()
								   ->name(qr/\A\d+\z/)
								   ->in($IN);
	@ti_files = sort {$a cmp $b} @ti_files;
	my @jgi_tis = map {path($_)->basename} @ti_files;
	#print Dumper(\@external_tis);

    #FIRST compare to ensembl_all and nr_genomes directories
    my $ens_path = path( path($OUT)->parent, 'ensembl_all' )->canonpath;
    my $nr_path  = path( path($OUT)->parent, 'nr_genomes' )->canonpath;

    my @ti_files_ens = File::Find::Rule->file()->name(qr/\A\d+\z/)->in($ens_path);
    #print Dumper(\@ti_files_ens);
    my %ens_tis = map { path($_)->basename => undef } @ti_files_ens;
    #print Dumper(\%ens_tis);

    my @ti_files_nr = File::Find::Rule->file()->name(qr/\A\d+\z/)->in($nr_path);
    my %nr_tis = map { path($_)->basename => undef } @ti_files_nr;

    my @not_found;
    foreach my $ti (@jgi_tis) {
        my $ti_orig_loc = path( $IN, $ti )->canonpath;
        if ( ( !exists $ens_tis{$ti} ) and ( !exists $nr_tis{$ti} ) ) {
            push @not_found, $ti_orig_loc;
        }
        else {
            $log->warn("Action: $ti_orig_loc excluded because found in ensembl_all or nr_genomes");
        }
    }

    #print Dumper(\@not_found);

	#SECOND check for existence in TI_FULLLIST and copy to external
	#insert query to insert to ti_full_list
	my $q_in = qq{
	INSERT INTO ti_full_list (ti, genes_cnt, source)
	VALUES (?, ?, 'JGI')
	};
	my $sth = $dbh->prepare($q_in);

	#select query to get ti for comparison to fasta_file
	my $q_sel = qq{
	SELECT ti FROM ti_full_list
	WHERE ti = ?
	};
	my $sth_sel = $dbh->prepare($q_sel);

	#starting iteration over @ti_files to copy genomes from $IN to $OUT
	foreach my $ti_file (@not_found) {
		$log->trace( "Action: working on $ti_file" );
		my $ti_from_file = path($ti_file)->basename;
		my $end_ti_file = path($OUT, $ti_from_file);
		
		#compare ti_from_file to ti in db to see if it exists
		$sth_sel->execute($ti_from_file);
		my ($ti_from_db) = $sth_sel->fetchrow_array();

		if ($ti_from_db) {
			$log->error("Ti file:$ti_file found in TI_FULLLIST");
			#delete ti_files (genomes) if they exist in $OUT dir and TABLE
			if (-f $end_ti_file) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked" );
			}
		}
		else {
			#unlink if already there
			if (-f $end_ti_file) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked" );
			}
			#copy fasta from $IN to $OUT and transform as needed
			$log->info( "Action: file $ti_file not found in modified list:$TI_FULLLIST" );
			my $fasta_cnt = collect_fasta_print({FILE => $ti_file, TAXID => $ti_from_file, %{$param_href} });

			#insert into ti_full_list
			eval {$sth->execute($ti_from_file, $fasta_cnt); };
			my $rows = $sth->rows;
			$log->debug( "Action: table $TI_FULLLIST inserted $rows rows for ti:$ti_from_file" ) unless $@;
			$log->error( "Action: inserting $TI_FULLLIST failed for ti:$ti_from_file:$@" ) if $@;
			if ($@) {
				unlink $end_ti_file and $log->warn( "Action: file $end_ti_file unlinked because it already exists in $TI_FULLLIST table" );
			}
		}
	}
	
	#UPDATE with species_names
    my $up_sp = qq{
	UPDATE $TI_FULLLIST AS ti
	SET ti.species_name = (SELECT DISTINCT na.species_name
	FROM $NAMES AS na WHERE ti.ti = na.ti)
	WHERE ti.species_name IS NULL;
    };
    eval { $dbh->do($up_sp, { async => 1 } ) };
	my $rows_up = $dbh->mysql_async_result;
    $log->debug( "Action: update to $TI_FULLLIST updated $rows_up rows!" ) unless $@;
    $log->error( "Action: updating $TI_FULLLIST failed: $@" ) if $@;

	#count genomes coming from JGI
	my $jgi_cnt = $dbh->selectrow_array("SELECT COUNT(*) FROM $TI_FULLLIST WHERE source = 'JGI'");
	$log->info("Report: found $jgi_cnt JGI genomes in table:$TI_FULLLIST");

	$sth->finish;
	$sth_sel->finish;
	$dbh->disconnect;

	return;
}


### INTERNAL UTILITY ###
# Usage      : my $fasta_cnt = collect_fasta_print({FILE => $ti_file, TAXID => $ti_from_file, %{$param_href} });
# Purpose    : accepts fasta location and gives fasta count
#            : transforms fasta to simpler form (only gene name in header) and checks fasta_seq for errors
# Returns    : $line_count (count of fasta records)
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : part of copy_external_genomes() mode
# See Also   : copy_external_genomes()
sub collect_fasta_print {
	my $log = Log::Log4perl::get_logger("main");
	$log->logcroak( 'collect_fasta_print() needs a $param_href' ) unless @_ == 1;
	my ( $param_href ) = @_;

	my $OUT      = $param_href->{OUT}   or $log->logcroak( 'no $OUT specified on command line!' );
	my $fasta_in = $param_href->{FILE}  or $log->logcroak( 'no FILE sent to sub!' );
	my $ti       = $param_href->{TAXID} or $log->logcroak( 'no TAXID sent to sub!' );
	my $BLAST_fmt= defined $param_href->{BLAST} ? $param_href->{BLAST} : undef ;

	open my $in_fh, "<", $fasta_in  or $log->logdie( "Error: can't open $fasta_in: $!" );
	my $fasta_out = path($OUT, $ti);
	open my $genome_ti_fh, ">", $fasta_out or $log->logdie( "Error: can't write to $fasta_out: $!" );
	
	#return $/ value to newline for $header_first_line
	#local $/ = "\n";

	#my $header_first_line = <$in_fh>;
	#print {$stat_fh} $header_first_line, "\t";
	
	#return to start of file
	#seek $extracted_fh, 0, 0;

	#look in larger chunks between records
	local $/ = ">";
	my $line_count = 0;
	while (<$in_fh>) {
		chomp;

		if (m{\A([^(\h\v)]+)      #gene name or gene id till first horizontal space or vertical space (already pruned fasta)
				(?:\h+)*          #optional horizontal space
				(?:[^\v]+)*\v     #optional description ending in vertical space + vertical space
				(.+)}xs) {        #fasta seq (everything after first vertical space (multiline mode=s)

			$line_count++;
			my $header = $1;
			my $fasta_seq = $2;
			$fasta_seq =~ s/\R//g;         #delete all vertical and horizontal space
			$fasta_seq = uc $fasta_seq;    #to uppercase
			if ($BLAST_fmt) {
			    $fasta_seq =~ tr{J}{*};    #change J to * for BLAST
			}
			else {
			    $fasta_seq =~ tr{*}{J};    #change * to J for cd-hit
			}
			$fasta_seq =~ tr{A-Z*}{}dc;    #delete all special characters
			#c Complement the SEARCHLIST.
			#d Delete found but unreplaced characters.

			print $genome_ti_fh ('>', $header, "\n", $fasta_seq, "\n");
		}
	}   #end while

	if ($line_count) {
		$log->debug( qq|Action: saved fasta file to $fasta_out with $line_count lines| );
		#print {$stat_fh} $line_count, "\n";
	}

	return $line_count;
}


### INTERFACE SUB ###
# Usage      : del_species_with_strain( $param_href );
# Purpose    : deletes TOATL species genomes that have subspecies or strain genomes (third step) from final table
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it works on ti_full_list table (doesn't create it)
#            : deletes hybrid genomes
#            : it deletes genomes that have species and strain genomes in full dataset
#            : (both nr and existing genomes)
# See Also   : del_total_genomes() - second step
#            : run del_total_genomes and copy_external_genomes to recreate ti_full_list
sub del_species_with_strain {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'del_species_with_strain() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $DATABASE = $param_href->{DATABASE}    or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $TI_FULLLIST = $TABLES{ti_full_list};
			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#FIRST PART: delete species that are hybrids
	my $del_hybrid = sprintf( q{
	DELETE ti FROM %s AS ti
	WHERE species_name LIKE %s }, $dbh->quote_identifier($TI_FULLLIST), $dbh->quote('%\_x\_%') );
    #say $del_hybrid;
	eval { $dbh->do($del_hybrid, { async => 1 } ) };
	my $rows_hy = $dbh->mysql_async_result;
	$log->debug( "Action: deleted $rows_hy hybrid species from $TI_FULLLIST") unless $@;
	$log->error( "Action: table $TI_FULLLIST delete for hybrids failed: $@" ) if $@;

	#SECOND PART: remove superfluous genomes
    my $species_query = qq{
    SELECT species_name 
    FROM $TI_FULLLIST
    ORDER BY species_name
    };
    my @species_names = map { $_->[0] } @{ $dbh->selectall_arrayref($species_query) };

	#get count of genus if there is more than 1 (into hash)
	my $found_genus_species = '';
	my %found_hash_cnt;
	foreach my $species (@species_names) {
		(my $genus_species = $species) =~ s/\A([^_]+)_(.+)_(?:.+)\z/$1_$2/g;

		if ($genus_species eq $found_genus_species) {
			$found_hash_cnt{$genus_species}++;   #add to hash only if found earlier (only duplicates)
		}
		$found_genus_species = $genus_species;   #now found has previous genus_species
	}
	#print Dumper(\%found_hash_cnt);

	#now get found species into hash of arefs to delete them later
	my (%hoarefs);
	foreach my $species2 (@species_names) {
		(my $genus_spec = $species2) =~ s/\A([^_]+)_(.+)_(?:.+)\z/$1_$2/g;

		if (exists $found_hash_cnt{$genus_spec}) {
			push @{ $hoarefs{$genus_spec} }, $species2;   #created Hash of array refs
		}
	}
	#print Dumper(\%hoarefs);

	#search for species to delete
	my $delete_spec = qq{
		DELETE ti FROM $TI_FULLLIST AS ti
		WHERE species_name = ?;
	};
	my $sth_del = $dbh->prepare($delete_spec, { async => 1 } );

	while (my ($spec_key, $grp_aref) = each %hoarefs) {
		my @spec_group = @{ $grp_aref };   #full group here
		my %hash_to_print = ($spec_key => $grp_aref);
		foreach my $spec (@{ $grp_aref }) {
			my @spec_search_group = map { /\A$spec\z/ ? () : $_} @spec_group;            #remove only species that is exactly the same
			@spec_search_group = map { /\A$spec(\d+)\z/ ? () : $_} @spec_search_group;   #remove species that is different only in ending number too
			@spec_search_group = map { /\A$spec([^_]+)\z/ ? () : $_} @spec_search_group; #remove species that is different only in ending letters after _
			if ( grep { /$spec/ } @spec_search_group) {                       #search for that species among the other species (anywhere in name)
				#$log->trace("Report: found match for:{$spec} in:{@spec_search_group}");
				print Dumper(\%hash_to_print);     #print group before delete
				$log->trace( "Action: deleting:$spec" );

				#DELETE species from ti_full_list table (because it is species with strains present)
				eval { $sth_del->execute($spec); };
				my $rows_del_spec = $sth_del->mysql_async_result;
			    $log->debug( "Action: table $TI_FULLLIST deleted $rows_del_spec rows for:{$spec}" ) unless $@;
			    $log->debug( "Action: deleting $TI_FULLLIST failed for:$spec: $@" ) if $@;
			}
			else {
				#$log->trace("Report: no match for:$spec in {@spec_search_group}");
			}
		}
	}
	
	#report the changes made
	my $genome_cnt = $dbh->selectrow_array("SELECT COUNT(*) FROM $TI_FULLLIST");
	$log->info("Report: found $genome_cnt genomes in table:$TI_FULLLIST");
	
	$dbh->disconnect;

	return;
}

### INTERFACE SUB ###
# Usage      : merge_existing_genomes( $param_href );
# Purpose    : prints present genomes to dropdox/D.../db../data/eukarya
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it takes genomes from old directory (base) and prints them to $OUT
# See Also   : 
sub merge_existing_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'merge_existing_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

	my $DATABASE = $param_href->{DATABASE} or $log->logcroak( 'no $DATABASE specified on command line!' );
	my $OUT      = $param_href->{OUT}      or $log->logcroak( 'no $OUT specified on command line!' );
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $TI_FULLLIST = $TABLES{ti_full_list};
	my $nr_dir   = path(path($OUT)->parent, 'nr_genomes');
	my $ens_dir  = path(path($OUT)->parent, 'ensembl_all');
	my $jgi_dir  = path(path($OUT)->parent, 'jgi_clean');
	my $ext_dir  = path(path($OUT)->parent, 'external');

			
	#get new handle
    my $dbh = dbi_connect($param_href);

	#work on all other sequences
	#get all tax_ids in TI_FULLLIST
	#(all larger than 99 - Centos6 and Boost issue) in MakePhyloDb
    my $tis_query = qq{
    SELECT ti, source
    FROM $TI_FULLLIST
	WHERE ti > 99
    ORDER BY ti
    };
    my %tis_with_source = map { $_->[0] => $_->[1] } @{ $dbh->selectall_arrayref($tis_query) };

	#clean $OUT dir before use
	if ( -d $OUT ) {
            path($OUT)->remove_tree and $log->warn(qq|Action: dir $OUT removed and cleaned|);
        }
    path( $OUT )->mkpath and $log->trace(qq|Action: dir $OUT created empty|);


	#SECOND PART: copy to $OUT (to all dir) if found in TI_FULLLIST table
	my $jgi_cnt = 0;
	my $nr_cnt = 0;
	my $ext_cnt = 0;
	my $ens_cnt = 0;
	foreach my $ti ( keys %tis_with_source ) {
		$log->trace( "Action: working on $ti" );

		my $ti_in_source_dir;
		foreach ($tis_with_source{$ti}) {
			when ('JGI') {
				$ti_in_source_dir = path(path($OUT)->parent, 'jgi_clean', $ti)->canonpath;

				#delete ti_files (genomes) if they exist in $OUT dir
				my $end_ti_file = path($OUT, $ti);
				if (-f $end_ti_file) {
					unlink $end_ti_file and $log->error( "Action: file $end_ti_file unlinked" );
				}

				#copy them from in_dir to $OUT
            	path($ti_in_source_dir)->copy($OUT) and $log->debug( "Action: file $ti_in_source_dir copied to $end_ti_file" );
				$jgi_cnt++;
			};
			when ('NCBI') {
				$ti_in_source_dir = path(path($OUT)->parent, 'nr_genomes', $ti)->canonpath;

				#delete ti_files (genomes) if they exist in $OUT dir
				my $end_ti_file = path($OUT, $ti);
				if (-f $end_ti_file) {
					unlink $end_ti_file and $log->error( "Action: file $end_ti_file unlinked" );
				}

				#copy them from in_dir to $OUT
            	path($ti_in_source_dir)->copy($OUT) and $log->debug( "Action: file $ti_in_source_dir copied to $end_ti_file" );
				$nr_cnt++;
			};
			when ('external') {
				$ti_in_source_dir = path(path($OUT)->parent, 'external', $ti)->canonpath;

				#delete ti_files (genomes) if they exist in $OUT dir
				my $end_ti_file = path($OUT, $ti);
				if (-f $end_ti_file) {
					unlink $end_ti_file and $log->error( "Action: file $end_ti_file unlinked" );
				}

				#copy them from in_dir to $OUT
            	path($ti_in_source_dir)->copy($OUT) and $log->debug( "Action: file $ti_in_source_dir copied to $end_ti_file" );
				$ext_cnt++;
			};
			when ('Ensembl') {
				$ti_in_source_dir = path(path($OUT)->parent, 'ensembl_all', $ti)->canonpath;

				#delete ti_files (genomes) if they exist in $OUT dir
				my $end_ti_file = path($OUT, $ti);
				if (-f $end_ti_file) {
					unlink $end_ti_file and $log->error( "Action: file $end_ti_file unlinked" );
				}

				#copy them from in_dir to $OUT
            	path($ti_in_source_dir)->copy($OUT) and $log->debug( "Action: file $ti_in_source_dir copied to $end_ti_file" );
				$ens_cnt++;
			};
			default { $log->warn( "Action: tax_id $ti not found in source directories" ); };


		}   #end foreach when

	}

	$log->info("Copied $jgi_cnt JGI genomes to $OUT");
	$log->info("Copied $nr_cnt NCBI genomes to $OUT");
	$log->info("Copied $ext_cnt external genomes to $OUT");
	$log->info("Copied $ens_cnt Ensembl genomes to $OUT");
	$dbh->disconnect;

	return;
}


### INTERFACE SUB ###
# Usage      : prepare_cdhit_per_phylostrata( $param_href );
# Purpose    : it splits database of genomes based on phylostrata and sends each phylostrata to cdhit
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it needs indir for genomes, phylo table for species and outdir per phylostrata
#            : it appends to per_ps_genome file (can be run for multiple indirs -> bacteria, archea and eukarya)
# See Also   : run first: perl blastdb_analysis.pl --mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 7955 -org dr -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
#            : for Danio rerio
sub prepare_cdhit_per_phylostrata {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('prepare_cdhit_per_phylostrata() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $IN       = $param_href->{IN}       or $log->logcroak('no $IN specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} } or $log->logcroak('no $TABLES specified on command line!');
    my $PHYLO    = $TABLES{phylo};

    #get new handle
    my $dbh = dbi_connect($param_href);

    #FIRST: get phylostrata from phylo table for specific organism
    my $select_ps_columns = qq{
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '$DATABASE' AND TABLE_NAME = '$PHYLO' AND ORDINAL_POSITION > 1
    };    #-- skip first column which is id auto_increment
    my @ps_columns = map { $_->[0] } @{ $dbh->selectall_arrayref($select_ps_columns) };
	my $ps_num = @ps_columns;
    $log->info(qq|Report: $ps_num phylostrata:{@ps_columns}|);

	#	#make a backup copy of PHYLO table
	#	my $ph_copy = create_table_copy( { ORIG => $PHYLO, %{$param_href} } );
	#
	#	#create copy of copy of $ORIG table (just in case)
	#	my $ph_backup = create_table_copy( { ORIG => $PHYLO, TO => "${PHYLO}_backup", %{$param_href} } );

    #collect all genomes from $IN
    my @ti_files = File::Find::Rule->file()->name(qr/\A\d+\.ff\z/)->in($IN);   #taxid.ff files created by MakePhyloDb
	@ti_files = sort @ti_files;

	#clean $OUT dir before use
	if ( -d $OUT ) {
            path($OUT)->remove_tree and $log->warn(qq|Action: dir $OUT removed and cleaned|);
        }
    path( $OUT )->mkpath and $log->trace(qq|Action: dir $OUT created empty|);

    #create outdir foreach phylostratum and copy genomes from that ps into it
    DIR:
    foreach my $ps (@ps_columns) {
		my $ps_path = path( $OUT, $ps );
        if ( -d $ps_path ) {
            path($ps_path)->remove_tree and $log->warn(qq|Action: dir $ps_path removed|);
        }
        path( $ps_path )->mkpath and $log->info(qq|Action: dir $ps_path created|);
		
		#if ti in this phylostratum copy it to this ps directory
        my $select_ti = sprintf( qq{
		SELECT %s
		FROM %s
		WHERE %s = ? },
            $dbh->quote_identifier($ps), $dbh->quote_identifier($PHYLO), $dbh->quote_identifier($ps)
        );
        my $sth = $dbh->prepare($select_ti);

        TAXID:
        foreach my $ti_file (@ti_files) {
            my $ti = path($ti_file)->basename;
			$ti =~ s/\A(\d+)\.ff\z/$1/;

            $sth->execute($ti);
			#say $select_ti;
            $sth->bind_col( 1, \my $tax_id, { TYPE => 'integer' } );
            $sth->fetchrow_arrayref();   #now $tax_id has ti num

            my $taxid_in_ps_dir = path( $ps_path, $ti );
            if ( -f $taxid_in_ps_dir ) {
                unlink $taxid_in_ps_dir and $log->warn(qq|Action: genome $taxid_in_ps_dir unlinked|);
            }

            if ($tax_id) {
                path($ti_file)->copy($ps_path) and $log->debug(qq|Action: File $ti_file copied to $ps_path|);
            }
        }

		#cat all files in one ps
		my $out_ps_full = path($OUT, $ps . '.fa');
		if (-f $out_ps_full) {
			#unlink $out_ps_full and $log->warn(qq|Action: ps_full_file $out_ps_full unlinked|);
			$log->warn(qq|Action: ps_full_file $out_ps_full exists, it will be appended|);
		}
		my @tis_in_psdir = File::Find::Rule->file()->name(qr/\A\d+\.ff\z/)->in($ps_path);
		my $cnt_per_ps = @tis_in_psdir;
		#my @ti_files = sort @ti_files;
		if (@tis_in_psdir) {
			catalanche(\@tis_in_psdir => $out_ps_full); 
			$log->info(qq|Action: concatenated $cnt_per_ps files to $out_ps_full|);
		}

		#clean ps directories
		if ( -d $ps_path ) {
            path($ps_path)->remove_tree and $log->warn(qq|Action: dir $ps_path removed|);
        }
		
		#create TORQUE scripts to run cdhit
		if ((-f $out_ps_full) and ($cnt_per_ps > 10)) {
			my $pbs_path = print_pbs_cdhit_script($ps, $out_ps_full);
			$log->info(qq|Action: TORQUE script printed to $pbs_path|) if $pbs_path;
		}
		else {
			$log->warn(qq|Report: $ps has $cnt_per_ps genomes and is excluded for cdhit|);
			my $like_cdhit_name = path($OUT, $ps);
			path($out_ps_full)->move($like_cdhit_name) and $log->info(qq|Action: File $out_ps_full renamed to $like_cdhit_name|);
		}
    }

	#	#run cleanup of $PHYLO table for all phylostrata that have no genomes
	#	my @fa_files = File::Find::Rule->file()->name(qr/\Aps\d+\.fa\z/)->in($OUT);
	#	#$log->trace("FA_FILES:@fa_files");
	#	my @ps_names = map { path($_)->basename } @fa_files;
	#	@ps_names = map { /(\Aps\d+)/ } @ps_names;
	#	@ps_names = sort @ps_names;
	#	$log->trace("PS_NAMES:@ps_names");
	#	my %ps_na = map {$_ => undef} @ps_names;
	#
	#	#say "PS_COLUMNS:@ps_columns";
	#	my %ps_col = map {$_ => undef} @ps_columns;
	#	my @drop_ps;
	#	foreach my $ps_col (sort keys %ps_col) {
	#		if (! exists $ps_na{$ps_col}) {
	#			push @drop_ps, $ps_col;
	#		}
	#	}
	#	#say "DROP_PS:@drop_ps";
	#	my $droplist = join ", ", map { "DROP COLUMN $_" } @drop_ps;
	#	#$log->trace( "DROPLIST:$droplist" );
	#
	#	my $del_list = join " AND ", map { "$_ IS NULL" } @ps_names;
	#	#$log->trace("DEL_LIST:$del_list");
	#
	#	my $alter_q = qq{
	#	ALTER TABLE $PHYLO $droplist 
	#	};
	#	$log->trace("$alter_q");
	#	eval{ $dbh->do($alter_q)};
	#	$log->error( "Action: altering table $PHYLO failed: $@" ) if $@;
	#	$log->info( "Action: table $PHYLO altered:{@drop_ps} dropped" ) unless $@;
	#
	#	my $del_q = qq{
	#	DELETE ph FROM $PHYLO AS ph
	#	WHERE $del_list
	#	};
	#	$log->trace("$del_q");
	#	my $del_rows;
	#	eval{ $del_rows = $dbh->do($del_q)};
	#	$log->error( "Action: deleting table $PHYLO failed: $@" ) if $@;
	#	$log->info( "Action: table $PHYLO deleted $del_rows rows" ) unless $@;
	#
	#    my $rows_left = $dbh->selectrow_array("SELECT COUNT(*) FROM $PHYLO");
	#    $log->info("Report: table $PHYLO has $rows_left rows");

    $dbh->disconnect;
    return;

}


### INTERNAL_UTILITY ###
# Usage      : my $ph_copy = create_table_copy( { ORIG => $PHYLO, %{$param_href} } );
# Purpose    : creates copy of table
# Returns    : name of table copy
# Parameters : ({ ORIG => $PHYLO, %{$param_href} })
# Throws     : croaks for parameters
# Comments   : used in prepare_cdhit_per_phylostrata()
# See Also   : prepare_cdhit_per_phylostrata()
sub create_table_copy {
	my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('create_table_copy() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ORIG     = $param_href->{ORIG}     or $log->logcroak('no $ORIG passed to sub!');
    my $TO       = $param_href->{TO};   #backup name

	my $dbh = dbi_connect($param_href);

    #name and drop table if exists
    my $copy = defined $TO ? "${TO}_$$" : "$ORIG" . '_copy';
    my $drop_q = qq{
    DROP TABLE IF EXISTS $copy
    };
    eval{ $dbh->do($drop_q)};
    $log->error( "Action: dropping table $copy failed: $@" ) if $@;
    $log->trace( "Action: table $copy dropped successfully!" ) unless $@;

    my $create_q = qq{
    CREATE TABLE $copy LIKE $ORIG
    };
    eval{ $dbh->do($create_q)};
    $log->error( "Action: creating table $copy failed: $@" ) if $@;
    $log->trace( "Action: table $copy created successfully!" ) unless $@;

    my $insert_q = qq{
    INSERT INTO $copy
    SELECT * FROM $ORIG
    };
	my $rows;
    eval{ $rows = $dbh->do($insert_q)};
    $log->error( "Action: inserting into table $copy failed: $@" ) if $@;
    $log->debug( "Action: table $copy inserted $rows rows!" ) unless $@;

	return $copy;
}

### INTERNAL_UTILITY ###
# Usage      : catalanche(\@tis_in_psdir => $out_ps_full);
# Purpose    : concatenates all files in dir to single file
# Returns    : nothing
# Parameters : catalanche(aref_of_files_in dir => $end_file);
# Throws     : nothing
# Comments   : used in prepare_cdhit_per_phylostrata()
#            : #by JDPORTER on http://www.perlmonks.org/?node_id=515106
# See Also   : prepare_cdhit_per_phylostrata()
sub catalanche {
    system qq( cat "$_" >> "$_[1]" ) for @{$_[0]};
    return;
}

### INTERNAL_UTILITY ###
# Usage      : my $pbs_path = print_pbs_cdhit_script($ps, $out_ps_full);
# Purpose    : creates pbs script for each phylostratum
#            : and writes cd-gt command to file (if needed for manual start or run_cdhit() )
# Returns    : path of PBS script
# Parameters : ($phylostratum, $out_dir)
# Throws     : croaks for parameters
# Comments   : used in prepare_cdhit_per_phylostrata()
# See Also   : prepare_cdhit_per_phylostrata()
sub print_pbs_cdhit_script {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('print_pbs_cdhit_script() needs $ps and $out_ps_full') unless @_ == 2;
	my ($ps, $out_ps_full) = @_;

	(my $out_cluster = $out_ps_full) =~ s/\.fa\z//g;

	my $cdhit_torque = <<"TORQUE";
#!/bin/bash


# job name
#PBS -N cdhit_$ps

#PBS -m e
#PBS -M msestak\@irb.hr

# queue:
#PBS -q default

# request resources (this is optional)
#
#PBS ncpus=24:mem=30gb


# executable line

/home/msestak/kclust/cdhit/cd-hit-v4.6.1-2012-08-27/cd-hit -i $out_ps_full -o $out_cluster -c 0.9 -n 5 -M 0 -T 0 -d 200

# setting the CD-HIT parameter -T 0, all CPUs defined in the SLURM script will be used.
# setting the parameter -M 0 allows unlimited usage of the available memory.
# setting the parameter -d 200 (length of header)

TORQUE

	my $pbs_path = path(path($out_ps_full)->parent, "$ps" . ".pbs");
	open my $pbs_fh, ">", $pbs_path or $log->logdie(qq|Error: can't write to $pbs_path|);
	say {$pbs_fh} $cdhit_torque;

	#print cd-hit command to screen and separate file
	my $cdhit_cmd = qq{/home/msestak/kclust/cdhit/cd-hit-v4.6.1-2012-08-27/cd-hit -i $out_ps_full -o $out_cluster -c 0.9 -n 5 -M 0 -T 0 -d 200};
	my $cmd_file  = path(path($out_ps_full)->parent, "cd_hit_cmds");
	open my $cmd_fh, ">>", $cmd_file or $log->logdie(qq|Error: can't write to $cmd_file|);
	say {$cmd_fh} $cdhit_cmd;
	$log->debug(qq|$cdhit_cmd|);

	return $pbs_path;

}


### INTERFACE SUB ###
# Usage      : run_cdhit( $param_href );
# Purpose    : it runs cd-hit from command line (not from PBS script)
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : it needs indir for genomes and outdir
#            : creates total db at end
# See Also   : run first: perl blastdb_analysis.pl --mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 7955 -org dr -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
sub run_cdhit {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('run_cdhit() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $INFILE   = $param_href->{INFILE}   or $log->logcroak('no $INFILE specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');

	my $cmd_file;
	$cmd_file = path($INFILE)->slurp or $log->logdie(qq|Report: Can't open cd-hit command-file $cmd_file|);
	my @cmd_lines = split("\n", $cmd_file);
	@cmd_lines = reverse @cmd_lines;   #start from last phylostratum

	foreach my $cmd (@cmd_lines) {
		chomp $cmd;
		(my $ps) = $cmd =~ m{(ps\d+)};
		my ($stdout_cd, $stderr_cd, $exit_cd) = capture_output( $cmd, $param_href );
			if ($exit_cd == 0) {
				my @lines = split("\n", $stdout_cd);
				my ($input_seqs1, $input_seqs2, $clusters, $memory_used, $cpu_time);
				for (@lines) {
					when (m/\Atotal seq:\s+(\d+)/) {$input_seqs1 = $1;}
					when (m/(\d+)\s+finished\s+(\d+)\s+clusters/) { $input_seqs2 = $1; $clusters = $2;}
					when (m/\AApprixmated maximum memory consumption:\s+(\d+)/) {$memory_used = $1;}
					when (m/\ATotal CPU time\s+(\d+\.\d+)/) {$cpu_time = $1;}
				}
				if ($input_seqs1 != $input_seqs2) {
					$log->error(qq|Report: some sequences skipped because of errors|);
				}

				$log->info(qq|Action: cd-hit for $ps finished (INPUT:$input_seqs2 sequences and OUT:$clusters clusters)\nMemory used:${memory_used}M\nTotal CPU time: $cpu_time sec|);

			}
			else {
				$log->error(qq|Action: $cmd failed:$stderr_cd|);
			}

			sleep 10;

	}

	return;

}

### INTERFACE SUB ###
# Usage      : get_ensembl_genomes( $param_href );
# Purpose    : collects tax_ids from tiktaalik and inserts it as a table in db
# Returns    : nothing
# Parameters : ( $param_href )
# Throws     : croaks for parameters
# Comments   : 
# See Also   : 
sub get_ensembl_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak( 'get_ensembl_genomes() needs a $param_href' ) unless @_ == 1;
    my ( $param_href ) = @_;

    my $ENGINE = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $IN       = $param_href->{IN}       or $log->logcroak('no $IN specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my %TABLES   = %{ $param_href->{TABLES} };
    my $NAMES    = $TABLES{names};

	#collect genomes from $IN
	my @ti_files = File::Find::Rule->file()
								   ->name(qr/\A\d+\z/)
								   ->in($IN);
	#say "FILES:@ti_files";
	#sort files by number (Schwartzian transform)
    my @sorted_files =
	    map { $_->[0] }                    #return full filename (aref position 0)
	    sort { $a->[2] <=> $b->[2] }       #sort by num (ref position 2)
        map { [ $_, /\A(.+)(\d+)\z/ ] }    #aref with [file, basename, num]
        @ti_files;
	#say "SORTED:", Dumper(\@sorted_files);

	#get new database handle
	my $dbh = dbi_connect($param_href);

	#insert tax_ids into the database
	my $table_ti = "ensembl_genomes";
    my $create_q = qq{
    CREATE TABLE $table_ti (
    ti INT UNSIGNED NOT NULL,
	genes_cnt INT UNSIGNED NOT NULL,
	species_name VARCHAR(200) NULL,
	source VARCHAR(20) NOT NULL,
    PRIMARY KEY(ti),
	KEY(genes_cnt),
	KEY(species_name)
    )ENGINE=$ENGINE CHARSET=ascii };
    eval { $dbh->do($create_q) };
	create_table( { TABLE_NAME => $table_ti, DBH => $dbh, QUERY => $create_q, %{$param_href} } );

	#set because VARCHAR(200) in species_name
	$dbh->{LongReadLen} = 200;

	#prepare insert ensembl_genomes query
	my $ins_q = qq{
	INSERT INTO $table_ti (ti, genes_cnt, source)
	VALUES( ?, ?, ? )
	};
	my $sth_ins = $dbh->prepare($ins_q);

	#iterate over files, count records and insert to table (ti, genes_cnt, species_name, 'Ensembl'):w
	foreach my $ti_file (@sorted_files) {
		my $ti = path($ti_file)->basename;
		#say "TI:$ti";

		my $fasta_cnt = 0;
		{   #count fasta records
			local $/ = ">";
			open my $ti_fh, "<", $ti_file or $log->logdie("Error: can't open $ti_file:$!");
			while (<$ti_fh>) {
				chomp;
				if (/\A(.+)\z/s) {
					$fasta_cnt++;
				}
			}
			close $ti_fh;
		}
		#say "Records:$fasta_cnt";

		#insert into table
		eval {$sth_ins->execute($ti, $fasta_cnt, 'Ensembl'); };
		my $rows_ins = $sth_ins->rows;
		$log->error("Action: failed insert to table $table_ti") if $@;
		$log->debug("Action: table $table_ti inserted for ti:{$ti} $rows_ins row") unless $@;
	}

	#species with changed tax_ids
	my %sp_changed = (
		#473542 => 'Caenorhabditis briggsae',
		245018 => 649756,
		1525716 => 1545044,
		#1525718 => 1545044,
		473542   => 6238
	);
	while (my ($old_ti, $new_ti) = each %sp_changed) {
		my $update_ch = qq{
		UPDATE $table_ti
		SET ti = $new_ti
		WHERE ti = $old_ti
		};
		eval { $dbh->do($update_ch, { async => 1 } ) };
		my $rows_ch = $dbh->mysql_async_result;
    	$log->debug( "Action: update to $table_ti for $old_ti to $new_ti updated $rows_ch rows!" ) unless $@;
    	$log->error( "Action: updating $table_ti for for $old_ti to $new_ti failed: $@" ) if $@;
	}

	#update for Hydra magnipapillata
	#species_excluded from NCBI but present in our db
    my %sp_ex_ncbi = (
        6085    => 'Hydra magnipapillata',
        1525718 => 'Paracoccus_sp ._39524',
    );
	while (my ($ti, $species_name) = each %sp_ex_ncbi) {
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

	#print to file for MakeTree
	my $ti_files_path = path($OUT, 'ensembl_genomes');
	open my $ti_fh, ">:encoding(ASCII)", $ti_files_path or $log->logdie("Error: can't write to $ti_files_path:$!");
	#select q for all tis
	my $tis_query = qq{
    SELECT ti
    FROM $table_ti
    };
    my @tis = map { $_->[0] } @{ $dbh->selectall_arrayref($tis_query) };
	foreach (@tis) {
		say {$ti_fh} $_;
	}

    #UPDATE species_name
    my $up_ens = qq{
	UPDATE $table_ti AS ti
	SET ti.species_name = (SELECT DISTINCT na.species_name
	FROM $NAMES AS na WHERE ti.ti = na.ti)
    };
    eval { $dbh->do($up_ens, { async => 1 } ) };
	my $rows_up = $dbh->mysql_async_result;
    $log->debug( "Action: update to $table_ti updated $rows_up rows!" ) unless $@;
    $log->error( "Action: updating $table_ti failed: $@" ) if $@;

	#report number of genomes in ensembl_genomes table
    my $rows_end = $dbh->selectrow_array("SELECT COUNT(*) FROM $table_ti");
    $log->info("Report: table $table_ti has $rows_end rows");

	$sth_ins->finish;
    $dbh->disconnect;
    return;
}


### INTERFACE SUB ###
# Usage      : jgi_download( $param_href );
# Purpose    : it downloads proteomes from JGI Metazome site
# Returns    : nothing
# Parameters : $param_href with $OUT
# Throws     : 
# Comments   : uses XML::Twig to parse XML file
#            : creates table that will store info from XML and dispaches internal subs to do all the work
# See Also   : 
sub jgi_download {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('jgi_download() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

	my %params_phytozome = %{$param_href};
	my %params_metazome = %{$param_href};
	my %params_fungi = %{$param_href};
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';
    my $table    = 'jgi_download';

	say "JGI_DOWNLOAD:before ALL:", Dumper($param_href);
    #get new handle
    my $dbh = dbi_connect($param_href);

    #report what are you doing
    $log->info("---------->Creating jgi_download table:$table");
    my $create_query = sprintf(
        qq{
    CREATE TABLE %s (
	id INT UNSIGNED AUTO_INCREMENT NOT NULL,
    label VARCHAR(200) NOT NULL,
	filename VARCHAR(100) NOT NULL,
	size VARCHAR(10) NOT NULL,
	sizeInBytes INT UNSIGNED NOT NULL,
    timestamp VARCHAR(100),
	project VARCHAR(100) NULL,
	md5 VARCHAR(100) NULL,
	url VARCHAR(200) NOT NULL,
	ti INT UNSIGNED NULL,
	species_name VARCHAR(200),
	genes_cnt INT UNSIGNED NULL,
	source VARCHAR(10) DEFAULT 'JGI',
    PRIMARY KEY(id),
    KEY(ti),
	KEY(filename),
	KEY(species_name)
    )ENGINE=$ENGINE CHARACTER SET=ascii }, $dbh->quote_identifier($table)
    );
    create_table( { TABLE_NAME => $table, DBH => $dbh, QUERY => $create_query, %{$param_href} } );

    #curl downloads cookie to use later
    save_cookie($param_href);

	#downloads and loads GOLD database to use later (as a source of taxids)
	set_gold_table($param_href);
	
	#say "JGI_DOWNLOAD:before PHYTOZOME_DOWNLOAD:", Dumper($param_href);
	#say "JGI_DOWNLOAD:before PHYTOZOME_DOWNLOAD:%params_phytozome", Dumper(\%params_phytozome);
	#download genomes and save them with taxid from gold table
	download_phytozome(\%params_phytozome);
	sleep 5;
	
	#say "JGI_DOWNLOAD:before METAZOME_DOWNLOAD:", Dumper($param_href);
	#say "JGI_DOWNLOAD:before METAZOME_DOWNLOAD:%params_metazome", Dumper(\%params_metazome);
	download_metazome(\%params_metazome);
	sleep 5;
	
	#say "JGI_DOWNLOAD:before FUNGI_DOWNLOAD:", Dumper($param_href);
	#say "JGI_DOWNLOAD:before FUNGI_DOWNLOAD:%params_fungi", Dumper(\%params_fungi);
	download_fungi(\%params_fungi);
	sleep 5;

    # http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi
    # http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=Metazome
    # http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=PhytozomeV10

    $dbh->disconnect;
    return;
}


### INTERNAL UTILITY ###
# Usage      : save_cookie( $param_href );
# Purpose    : downloads cookie from JGI and modiefies to for second site (for fungi)
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
# Usage      : set_gold_table( $param_href );
# Purpose    : downloads GOLD excel file and imports it to database
# Returns    : nothing
# Parameters : needs $OUT
# Throws     : croaks for parameters
# Comments   : needed for jgi_download()
# See Also   : jgi_download()
sub set_gold_table {
	my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('save_cookie() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $ENGINE   = defined $param_href->{ENGINE} ? $param_href->{ENGINE} : 'InnoDB';

	#download gold table
	my $cookie_path = path($OUT, 'cookie_jgi');
	my $gold_file = path($OUT, 'goldData.xls');
	if (-f $gold_file) {
		unlink $gold_file and $log->warn("Action: GOLD file $gold_file unlinked");
	}
	my $cmd = qq{curl -C - https://gold.jgi.doe.gov/downloadexceldata  -c $cookie_path > $gold_file};   #-C - (for continue)
	my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
	if ( $exit == 0 ) {
	    $log->info("Action: GOLD file JGI saved at $gold_file");
	}
	else {
	    $log->error("Action: failed to save GOLD file from JGI:\n$stderr");
	}

	#process GOLD file (strange ending)
	my $gold_file_out = path($OUT, 'goldData.tsv');
	if (-f $gold_file_out) {
		unlink $gold_file_out and $log->warn("Action: perl GOLD file $gold_file_out unlinked");
	}
	{
		local $/ = "\t\n";
		open my $gold_in_fh, "<", $gold_file  or $log->logdie(qq|Report: Can't open GOLD file $gold_file:$!|);
	    open my $gold_out_fh, ">", $gold_file_out or $log->logdie(qq|Report: Can't open GOLD file $gold_file_out for writing:$!|);
	    my $gold_file_cnt = 0;
		while (defined (my $line = <$gold_in_fh>)) {
			chomp $line;
			if ($line =~ m{\ALast run:(.+)\z}) {
				my $last_run = $1;
				$log->warn("Report: GOLD file created at $last_run date");
			}
			else {
				say {$gold_out_fh} $line;
				$gold_file_cnt++;
			}
		}
		$log->info("Report: processed $gold_file_cnt lines in GOLD file:$gold_file_out");
	}


    #get new handle
    my $dbh = dbi_connect($param_href);

	my $gold_tbl = 'gold_ver5';
	my $create_q = qq{
	CREATE TABLE $gold_tbl (
	goldstamp VARCHAR(10) NOT NULL,
	legacy_goldstamp VARCHAR(10) NULL,
	project_name VARCHAR(200) NULL,
	ncbi_project_name VARCHAR(500) NULL,
	ncbi_project_id INT UNSIGNED NULL,
	project_type VARCHAR(50) NULL,
	project_status VARCHAR(50) NULL,
	sequencing_status VARCHAR(20) NULL,
	sequencing_centers VARCHAR(500) NULL,
	funding VARCHAR(2200) NULL,
	contact_name VARCHAR(40) NULL,
	ti INT UNSIGNED NOT NULL,
	domain VARCHAR(20) NULL,
	kingdom VARCHAR(20) NULL,
	phylum VARCHAR(50) NULL,
	class VARCHAR(50) NULL,
	spec_order VARCHAR(50) NULL,
	family VARCHAR(50) NULL,
	genus VARCHAR(50) NULL,
	species_name VARCHAR(200) NOT NULL,
	PRIMARY KEY(goldstamp),
	KEY(ti),
	KEY(species_name),
	KEY(project_type)
    )ENGINE=$ENGINE CHARACTER SET=ascii };
    create_table( { TABLE_NAME => $gold_tbl, DBH => $dbh, QUERY => $create_q, %{$param_href} } );

	#load file into db
	my $load_q = qq{
	LOAD DATA INFILE '$gold_file_out'
	INTO TABLE $gold_tbl
	IGNORE 1 LINES
	};
	$log->trace("Report: GOLD table load:$load_q");
    eval { $dbh->do($load_q, { async => 1 } ) };
	my $rows_l = $dbh->mysql_async_result;
    $log->debug( "Action: $gold_tbl loaded with $rows_l rows!" ) unless $@;
    $log->error( "Action: loading $gold_tbl failed: $@" ) if $@;

    #update with missed species
    my %to_update = (
        1670617 => 'Kalanchoe_laxiflora',
		#436017  => 'Ostreococcus_lucimarinus_CCE9901',
        436017  => 'Olucimarinus',
		296587  => 'MpusillaRCC299',
		#564608  => 'Micromonas_pusilla_CCMP1545',
        564608  => 'MpusillaCCMP1545',
		4155    => 'Mimulus_guttatus',
		#574566  => 'Coccomyxa_subellipsoidea_C-169',
        574566  => 'CsubellipsoideaC169',
        264402  => 'Capsella_grandiflora',
		#3711    => 'Brassica_rapa',
        3711    => 'BrapaFPsc',
		671525  => 'Sida fallax',
		4556    => 'Setaria viridis',

		37653   => 'Obimaculoides',
		6087    => 'Hmagnipapillata',
		9615    => 'Cfamiliaris',

        559307  => 'Zygosaccharomyces rouxii CBS732',


    );

	my $ins_q = qq{
	INSERT INTO $gold_tbl (goldstamp, ti, species_name)
	VALUES (?, ?, ?)
	};
	my $sth_ins = $dbh->prepare($ins_q);
	
	my $goldstamp = 0;
	while (my ($ti, $species) = each %to_update) {
		$goldstamp++;
		$sth_ins->execute($goldstamp, $ti, $species);
		$log->trace("Action: inserted species:{$species} to $gold_tbl");
	}

    $dbh->disconnect;
    return;
}


### INTERNAL UTILITY ###
# Usage      : download_phytozome( $param_href );
# Purpose    : downloads genomes from Phytozome portion of JGI
# Returns    : nothing
# Parameters : $param_href
# Throws     : 
# Comments   : first part of jgi_download mode
# See Also   : jgi_download()
sub download_phytozome {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('download_phytozome() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;
	no warnings 'uninitialized';

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

		#skip unwanted divisions and go into {early_release} folder
		foreach ($species_name) {
			when (/global_analysis/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/orthology/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/inParanoid/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/early_release/) {
				$log->warn("Action: working in $species_name");
            	my @early_folders = $folder_upper->children;

				EARLY:
				foreach my $early_folder (@early_folders) {
					my $early_name = $early_folder->att('name');
    			    $log->warn("Action: working in $early_name");
					#now working with species folders
					my @sp_folders = $early_folder->children;

					EARLY_SPECIES:
					foreach my $sp_folder (@sp_folders) {
						my $sp_name = $sp_folder->att('name');

						#skip unwanted divisions and go into {annotation} folder
						if ($sp_name =~ /annotation/) {
							$log->warn("Action: working in $sp_name");
							list_xml_folders( { FOLDER => $sp_folder, %{$param_href} } );
						}
					}   #end EARLY_SPECIES
				}   #end EARLY
			}   #end early_release

			when(/.+/) {
				$log->warn("Action: working in $species_name");
            	my @species_folders = $folder_upper->children;

				SPECIES:
				foreach my $species_folder (@species_folders) {
					my $real_name = $species_folder->att('name');

					#skip unwanted divisions and go into {Annotation} folder
    			    if ($real_name =~ /annotation/) {
    			        $log->warn("Action: working in $real_name");
						list_xml_folders( { FOLDER => $species_folder, %{$param_href} } );
					}
				}
			}   #end species_folders
		}
	}


	#download proteomes using tis from jgi_download table
	#curl_genomes($param_href);                          #UNCOMMENT when running separately
}


### INTERNAL UTILITY ###
# Usage      : download_metazome( $param_href );
# Purpose    : downloads genomes from Metazome portion of JGI
# Returns    : nothing
# Parameters : $param_href
# Throws     : 
# Comments   : second part of jgi_download mode
# See Also   : jgi_download()
sub download_metazome {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('download_metazome() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');

    #get new handle
    my $dbh = dbi_connect($param_href);

	my $URL = q{http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=Metazome};
	
	my ($xml_name, $xml_path) = get_jgi_xml( { URL => $URL, OUT => $OUT } );

	#say "DOWNLOAD_METAZOME:xml_path:$xml_path";
	my $twig= new XML::Twig(pretty_print => 'indented');
	$twig->parsefile( $xml_path );			# build the twig
	
	my $root= $twig->root;					# get the root of the twig
	my @folders_upper = $root->children;    # get the folders list
	
	UPPER:
	foreach my $folder_upper (@folders_upper) {
		my $species_name = $folder_upper->att( 'name' );
		$log->debug("FOLDER_UPPER-NAME:{$species_name}");

		#skip unwanted divisions and go into {early_release} folder
		foreach ($species_name) {
			when (/global_analysis/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/orthology/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/inParanoid/) { $log->trace("Action: skipped upper_folder:$species_name") and next UPPER; }
			when (/early_release/) {
				$log->warn("Action: working in $species_name");
            	my @early_folders = $folder_upper->children;

				EARLY:
				foreach my $early_folder (@early_folders) {
					my $early_name = $early_folder->att('name');
    			    $log->warn("Action: working in $early_name");
					#now working with species folders
					my @sp_folders = $early_folder->children;

					EARLY_SPECIES:
					foreach my $sp_folder (@sp_folders) {
						my $sp_name = $sp_folder->att('name');

						#skip unwanted divisions and go into {annotation} folder
						if ($sp_name =~ /annotation/) {
							list_xml_folders( { FOLDER => $early_folder, %{$param_href} } );
						}
					}   #end EARLY_SPECIES
				}   #end EARLY
			}   #end early_release

			when(/.+/) {
				$log->warn("Action: working in $species_name");
            	my @species_folders = $folder_upper->children;

				SPECIES:
				foreach my $species_folder (@species_folders) {
					my $real_name = $species_folder->att('name');

					#skip unwanted divisions and go into {Annotation} folder
    			    if ($real_name =~ /annotation/) {
    			        $log->warn("Action: working in $real_name");
						list_xml_folders( { FOLDER => $species_folder, %{$param_href} } );
					}
				}
			}   #end species_folders
		}
	}


	#download proteomes using tis from jgi_download table
	#curl_genomes($param_href);                          #UNCOMMENT when running separately
}


### INTERNAL UTILITY ###
# Usage      : download_fungi( $param_href );
# Purpose    : downloads genomes from Metazome portion of JGI
# Returns    : nothing
# Parameters : $param_href
# Throws     : 
# Comments   : second part of jgi_download mode
# See Also   : jgi_download()
sub download_fungi {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('download_fungi() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');

    #get new handle
    my $dbh = dbi_connect($param_href);

    my $URL = q{http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi};

    my ( $xml_name, $xml_path ) = get_jgi_xml( { URL => $URL, %{$param_href} } );

    my $twig = new XML::Twig( pretty_print => 'indented' );
    $twig->parsefile($xml_path);    # build the twig

    my $root          = $twig->root;        # get the root of the twig
    my @folders_upper = $root->children;    # get the folders list

  UPPER:
    foreach my $folder_upper (@folders_upper) {
        my $folder_name = $folder_upper->att('name');
        $log->debug("FOLDER_UPPER-NAME:{$folder_name}");

        #skip unwanted divisions and go into {Files} folder
        if ($folder_name =~ /Files/) {
            $log->warn("Action: working in $folder_name");
            my @files_folders = $folder_upper->children;

			FILES:
			foreach my $files_folder (@files_folders) {
				my $folder_files_name = $files_folder->att('name');
				
				#skip unwanted divisions and go into {Annotation} folder
				if ($folder_files_name =~ /Annotation/) {
					$log->warn("Action: working in $folder_files_name");
					my @annot_folders = $files_folder->children;

					ANNOTATION:
					foreach my $annot_folder (@annot_folders) {
						my $annot_folder_name = $annot_folder->att('name');

						#skip unwanted divisions and go into {Filtered Models} folder
						if ($annot_folder_name =~ /Filtered Models/) {
							$log->warn("Action: working in $annot_folder_name");
							my @model_folders = $annot_folder->children;

							MODELS:
							foreach my $model_folder (@model_folders) {
								my $model_folder_name = $model_folder->att('name');

								#skip unwanted divisions and go into {Filtered Models} folder
								if ($model_folder_name =~ /Proteins/) {
									$log->warn("Action: working in $model_folder_name");
									list_xml_folders( { FOLDER => $model_folder, %{$param_href} } );
								}
							}   #end MODELS
						}
					}   #end ANNOTATION
				}
			}   #end FILES
		}
	}   #end UPPER








    #download proteomes using tis from jgi_download table
	curl_genomes($param_href);                           #always UNCOMMENTED because it runs last
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

    my $OUT = $param_href->{OUT} or $log->logcroak('no $OUT specified on command line!');
    my $URL = $param_href->{URL} or $log->logcroak('no $URL found in sub invocation!');
    my $cookie_path = path( $OUT, 'cookie_jgi' );
    ( my $xml_name = $URL ) =~ s{\A(?:.+?)organism=(.+)\z}{$1};
    my $xml_path = path( $OUT, $xml_name . '.xml' )->canonpath;
	#say "GET_JGI_XML:xml_path:$xml_path";

	CMD: {
		my $cmd = qq{curl -C - --retry 999 --retry-max-time 0 $URL -b $cookie_path -c $cookie_path > $xml_path};
		my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
    	if ( $exit == 0 ) {
    	    $log->debug("Action: XML $xml_name from JGI saved at $xml_path");

    	    #check for zero size
    	    if ( -z $xml_path ) {
				state $i = 0;
				$i++;
    	        $log->error("ZERO size: $xml_path Going to redo:$i");
				sleep 1;
				redo CMD if $i < 101;
				last CMD if $i == 100;
    	    }
    	}
    	else {
    	    $log->error("Action: failed to save $xml_name from JGI:\n$stderr");
    	}
	}   #end CMD block

    return $xml_name, $xml_path;
}


### INTERNAL UTILITY ###
# Usage      : list_xml_folders( $param_href );
# Purpose    : lists folders with species and grabs files from them
# Returns    : hash ref of params for get_jgi_genome()
# Parameters : $param_href
# Throws     :
# Comments   : part of jgi_download mode
# See Also   : jgi_download()
sub list_xml_folders {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('list_xml_folders() needs a $folder_upper') unless @_ == 1;
    my ($param_href) = @_;
	no warnings 'uninitialized';

    my $folder = $param_href->{FOLDER} or $log->logcroak('no $OUT specified on command line!');
    my @files = $folder->children;

    #say "FILES:", Dumper(\@files);
	#say "LISTING FILES:@files";

    foreach my $file (@files) {
        my $filename = $file->att('filename');
        if (   ( $filename =~ m{protein.fa.gz\z} )
            or ( $filename =~ m{peptide.fa.gz\z} )
            or ( $filename =~ m{aa.fasta.gz\z} ) ) {    #first for Phytozome, second for Metazome, third for fungi
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
            $project = $label if ( ( $project eq '' ) or ( $project == 0 ) );    #empty or 0 doesn't work
			#say "project:{$project}";
            my $md5 = $file->att('md5');
            $md5 = 'none' if ( ! defined $md5 );    #fungi don't have md5

			#say "md5:$md5";
            my $url = $file->att('url');

            #say "url:$url";
            $url =~ s{/ext-api(?:.+?)url=(.+)}{$1};

            #say $url;
            $url = 'http://genome.jgi.doe.gov' . $url;

            #say $url;
            get_jgi_genome({
					LABEL       => $label,
                    FILENAME    => $filename,
                    SIZE        => $size,
                    SIZEINBYTES => $size_in_bytes,
                    TIMESTAMP   => $timestamp,
                    PROJECT     => $project,
                    MD5         => $md5,
                    URL         => $url,
                    %{$param_href}
                });
        }
    }
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
    my %TABLES      = %{ $param_href->{TABLES} };
    my $GOLD_TBL    = $TABLES{gold};
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
	(my $species_name_from_label = $LABEL) =~ s{\A(.+)(?:\s+v\d+\.\d+)\z}{$1};
	(my $species_name_from_label_ = $species_name_from_label) =~ tr/ /_/;

	#prepare select species_name from GOLD
	my $get_species_name = qq{
	SELECT DISTINCT species_name
	FROM $GOLD_TBL
	WHERE species_name LIKE '$species_pattern'
	OR species_name LIKE '$species_name_from_label'
	};
	say $get_species_name;
	my @species_gold = map { $_->[0] } @{ $dbh->selectall_arrayref($get_species_name) };

	#retrieve ti by species_name from GOLD
	my $get_ti = qq{
	SELECT DISTINCT ti
	FROM $GOLD_TBL
	WHERE species_name = ?
	};
	my $sth = $dbh->prepare($get_ti);

	#prepare select from names if not found in GOLD table
	my $get_na_species = qq{
	SELECT DISTINCT species_name
	FROM $NAMES
	WHERE species_name LIKE '$species_name_from_label_'
	};
	say "SPECIES_NA:$get_na_species";
	my @species_na = map { $_->[0] } @{ $dbh->selectall_arrayref($get_na_species) };
	say "NA_SPECIES:@species_na";

	my $get_na_ti = qq{
	SELECT DISTINCT ti
	FROM $NAMES 
	WHERE species_name = ?
	};
	my $sth_na = $dbh->prepare($get_na_ti);

	my ($ti, $species);
	if (scalar @species_gold == 0) {
		#try NAMES species
		if (scalar @species_na == 0) {
			$species = prompt "Write (with underscore) species you want to retrieve (SKIP: press ENTER)",
				">";
			$ti = prompt "Write ti of species",
				">";
			$log->info("RAW SPECIES:$species with ti:$ti");
		}
		elsif (scalar @species_na == 1) {
			($species) = @species_na;
			$sth_na->execute($species);
			$sth_na->bind_col(1, \$ti);
			$sth_na->fetchrow_arrayref();
			$log->info("SPECIES $species with ti:$ti");
		}
		else {
			$species = prompt "which species you want to retrieve",
				-menu => [@species_na],
				-number,
				">";
			$sth_na->execute($species);
			$sth_na->bind_col(1, \$ti);
			$sth_na->fetchrow_arrayref();
			$log->info("SPECIES $species with ti:$ti");
		}
	}


	#GOLD table match
	elsif (scalar @species_gold == 1) {
		($species) = @species_gold;
		$sth->execute($species);
		$sth->bind_col(1, \$ti);
		$sth->fetchrow_arrayref();
		$log->info("SPECIES $species with ti:$ti");
	}
	else {
		$species = prompt "which species you want to retrieve",
			-menu => [@species_gold],
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

		$species =~ tr/ /_/;   #be consistent with other species_names
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
                $log->error(qq|Report: insert failed for:$species_error ($@)|);

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
# Usage      : curl_genomes( $param_href );
# Purpose    : downloads genome from JGI using info from jgi_download table
#            : and saves genome as taxid
# Returns    : nothing
# Parameters : needs $OUT
# Throws     : 
# Comments   : needed for jgi_download()
# See Also   : jgi_download()
sub curl_genomes {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('curl_genomes() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $DATABASE = $param_href->{DATABASE} or $log->logcroak('no $DATABASE specified on command line!');
    my $cookie_path = path( $OUT, 'cookie_jgi' );

    #get new handle
    my $dbh = dbi_connect($param_href);

    #search by species_name from filename
    my $get_species = qq{
	SELECT ti, species_name, url, filename
	FROM jgi_download
	};
    my %ti_species = map { $_->[0], [ $_->[1], $_->[2], $_->[3] ] } @{ $dbh->selectall_arrayref($get_species) };
    my $cnt_species_pairs = keys %ti_species;
    $log->info("Report: Found $cnt_species_pairs ti->[species_name-url-filename] pairs");

	#insert into jgi_download $fasta_cnt
	my $upd_q = qq{
	UPDATE jgi_download
	SET genes_cnt = ?, source = 'JGI'
	WHERE ti = ?
	};
	my $sth_up = $dbh->prepare($upd_q);

  SPECIES:
    while ( my ( $ti, $species_ref ) = each %ti_species ) {
        my $species_name = $species_ref->[0];
        my $url          = $species_ref->[1];
        my $filename     = $species_ref->[2];
        my $jgi_out      = path(path($OUT)->parent, 'jgi');
        my $gzip         = path( $jgi_out, $filename )->canonpath;

        my $cmd = qq{curl --retry 999 --retry-max-time 0 -C - $url -b $cookie_path -c $cookie_path > $gzip};
        my ( $stdout, $stderr, $exit ) = capture_output( $cmd, $param_href );
        if ( $exit == 0 ) {
            $log->debug("Action: species: $species_name from JGI saved at $gzip");

			my $ae = Archive::Extract->new( archive => "$gzip" );
			my $ae_path;
			my $ok = do {
				$ae->extract(to => $jgi_out) or $log->logdie( $ae->error );
				my $ae_file = $ae->files->[0];
				$ae_path = path($jgi_out, $ae_file);
				$log->info( "Action: extracted to $ae_path" );
			};
			#delete gziped file
			unlink $gzip and $log->trace( qq|Action: unlinked $gzip| );

			#save genome under ti and count fasta records
			$param_href->{OUT} = $jgi_out;
			my $fasta_cnt = collect_fasta_print({FILE => $ae_path, TAXID => $ti, %{$param_href}});
			unlink $ae_path and $log->trace( qq|Action: unlinked $ae_path| );
			#say "FASTA_COUNT:$fasta_cnt";
			#if ($fasta_cnt < 100) {
			#	$log->error("Error: failed to download $url for $species_name with fasta:{$fasta_cnt}");
			#	redo SPECIES;
			#}

			#insert fasta count into jgi_download table
			eval { $sth_up->execute($fasta_cnt, $ti); };
			my $rows_up = $sth_up->rows;
			$log->error("Action: failed update to table:jgi_download for:{$species_name}") if $@;
			$log->debug("Action: table jgi_download updated $rows_up row(s) for:{$species_name}") unless $@;
        }
        else {
            $log->error("Action: failed to save $gzip from JGI:\n$stderr");
        }

    }

    return;
}



### INTERFACE SUB ###
# Usage      : cdhit_merge( $param_href );
# Purpose    : it merges all cdhit output files into one BLAST db
# Returns    : nothing
# Parameters : ( $param_href ) $IN and $OUTFILE
# Throws     : 
# Comments   : it replaces J to * in fasta file
# See Also   : prepare_cdhit() and run_cdhit()
sub cdhit_merge {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('cdhit_merge() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $IN      = $param_href->{IN}      or $log->logcroak('no $IN specified on command line!');
    my $OUT     = $param_href->{OUT}     or $log->logcroak('no $OUT specified on command line!');
    my $OUTFILE = $param_href->{OUTFILE} or $log->logcroak('no $OUTFILE specified on command line!');

	#delete and create $OUT
	if ( -d $OUT ) {
            path($OUT)->remove_tree and $log->trace(qq|Action: dir $OUT removed and cleaned|);
        }
    path( $OUT )->mkpath and $log->trace(qq|Action: dir $OUT created empty|);

    #collect all cdhit output files
    my @cdhit_in = File::Find::Rule->file()->name(qr/\Aps\d+\z/)->in($IN);
	#say "@cdhit_in";

	#open $OUTFILE for writing (delete if it exists because it appends
	if (-f $OUTFILE) {
		unlink $OUTFILE and $log->warn("Action: $OUTFILE exists. Unlinked!");
	}
    open my $fasta_all_fh, ">>", $OUTFILE or $log->logdie("Error: can't open file for writing:$OUTFILE $!");

	#hash of arefs to store full database and print it later
	my %db_fasta;

    #read each file, count fasta and append to OUTFILE
    my $total_cnt = 0;
    foreach my $ps_file (@cdhit_in) {
        open my $in_fh, "<", $ps_file or $log->logdie("Error: can't open file for reading:$ps_file $!");
		$log->info("Report: working on $ps_file");

		FASTA: {
            #look in larger chunks between records
            local $/ = ">";
            my $line_cnt = 0;
            while (<$in_fh>) {
                chomp;
				next if ($_ eq '');

                if (m{\A(pgi\|\d+\|ti\|(\d+)\|(?:[^\v]+))        #pgi id till vertical whitespace
						\v                #vertical space
						(.+)}xs           #fasta seq (everything after first vertical space (multiline mode=s)
                   ) {

                    $line_cnt++;
                    my $header      = $1;
					my $ti          = $2;
                    my $fasta_seq   = $3;   #put captures to variables ASAP!!! then clean them
					$header         =~ s/\t+/ /g;       #delete tab between pgi_id and gene_name (blast error with tab in header)
					my $full_header = '>' . $header;    #needed for db_fasta
                    $fasta_seq      =~ s/\R//g;         #delete all vertical and horizontal space
                    $fasta_seq      = uc $fasta_seq;    #to uppercase
                    $fasta_seq      =~ tr{J}{*};        #return J to * for BLAST

                    print {$fasta_all_fh} $full_header, "\n", $fasta_seq, "\n";

					#push fasta into hash (all sequences for specific organism into one array_ref)
					push @{ $db_fasta{$ti} }, $full_header . "\n" . $fasta_seq . "\n";
					#say Dumper(\%db_fasta);

					#say Dumper(@{ $db_fasta{$ti} });

					#exit if $. > 10;

                }
            }    #end while

            if ($line_cnt) {
                $log->debug(qq|Action: cdhit fasta file:$ps_file with $line_cnt lines appended to $OUTFILE|);
                $total_cnt += $line_cnt;
            }
        }   #end FASTA
    }    #end foreach printing fasta

    $log->info("Report: printed $total_cnt fasta records to $OUTFILE");

	#print all genomes to $OUT
	foreach my $ti_h (keys %db_fasta) {
		my $ti_path = path($OUT, $ti_h)->canonpath;
		open my $ti_fh, ">", $ti_path or $log->logdie("Error: can't open $ti_path for writing:$!");
		say {$ti_fh} @{ $db_fasta{$ti_h} };
		$log->trace("Action: written to $ti_path");
	}

	my $ti_cnt = keys %db_fasta;
	$log->info("Report: printed $ti_cnt genomes to $OUT");

    return;
}

### INTERFACE SUB ###
# Usage      : manual_add_fasta( $param_href );
# Purpose    : if cleans genome fasta sequences and it adds it to directory of interest
# Returns    : fasta_cnt
# Parameters : ( $param_href ) $OUT and $INFILE AND $TAXID
# Throws     : 
# Comments   : it takes genome from INFILE and transforms it to BLAST format (with *)
# See Also   : collect_fasta_print() does all the work
sub manual_add_fasta {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('manual_add_fasta() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $INFILE   = $param_href->{INFILE}   or $log->logcroak('no $INFILE specified on command line!');
    my $OUT      = $param_href->{OUT}      or $log->logcroak('no $OUT specified on command line!');
    my $TAXID   = $param_href->{TAXID}     or $log->logcroak('no $TAXID specified on command line!');

    my $fasta_cnt = collect_fasta_print({FILE => $INFILE, TAXID => $TAXID, BLAST => 1, %{$param_href} });

	$log->info("Report: transformed $INFILE to $OUT/$TAXID ($fasta_cnt rows) with BLAST_format = true");

    return;
}


### INTERFACE SUB ###
# Usage      : del_after_analyze( $param_href );
# Purpose    : it deletes genomes that are found in directory of chice but are not found in AnalysePhyloDb output
#            : probably put at 0 in nodes file
# Returns    : nothing
# Parameters : ( $param_href ) $IN for genomes directory, $INFILE for AnalysePhyloDb file and $OUT (deleted genomes destination)
# Throws     : 
# Comments   : genomes are not deleted but transfered to all_sync directory
# See Also   : 
sub del_after_analyze {
    my $log = Log::Log4perl::get_logger("main");
    $log->logcroak('del_after_analyze() needs a $param_href') unless @_ == 1;
    my ($param_href) = @_;

    my $IN     = $param_href->{IN}     or $log->logcroak('no $IN specified on command line!');
    my $INFILE = $param_href->{INFILE} or $log->logcroak('no $INFILE specified on command line!');
    my $OUT    = $param_href->{OUT}    or $log->logcroak('no $OUT specified on command line!');

    #collect all cdhit output files
    my @cdhit_in = File::Find::Rule->file()->name(qr/\A\d+\.ff\z/)->in($IN);
	my $files_cnt = @cdhit_in;
	$log->info("Report: found $files_cnt genomes in $IN");

	#get all taxids from AnalysePhyloDb output file
	open my $analyze_fh, "<", $INFILE or $log->logdie("Error: can't open $INFILE for reading:$!");
	my @tis;
	while (<$analyze_fh>) {
		chomp;

		if (/\A<ps>.+\z/) {
			$log->trace($_);
		}
		else {
			#line with genome info
			my (undef, undef, undef, $ti) = split /\t+/, $_;
			push @tis, $ti;
		}

	}

	#put taxids as heys of hash for fast check of existence
	my %tis_analyze = map {$_ => undef} @tis;
	my $lines_cnt = @tis;
	$log->info("Report: found $lines_cnt genomes in $INFILE");
	
	#move genomes not found in analyze file to #OUT
	my $moved_cnt = 0;
	foreach my $genome (@cdhit_in) {
		my $ti_gen = path($genome)->basename;
		$ti_gen =~ s/\.ff//;

		if (exists $tis_analyze{$ti_gen}) {
			#nothing
		}
		elsif (! exists $tis_analyze{$ti_gen}) {
			my $moved_genome = path($OUT, $ti_gen)->canonpath;
			path($genome)->move($moved_genome) and $log->debug("Action: moved $genome to $moved_genome");
			$moved_cnt++;
		}
		else {
			$log->error("Error: $genome not found in AnalysePhyloDb $INFILE");
		}
	}

	$log->info("Report: removed $moved_cnt genomes out of $IN to $OUT");

    return;
}






1;
__END__
=encoding utf-8

=head1 NAME

CollectGenomes - Downloads genomes from Ensembl FTP (and NCBI nr db) and builds BLAST database (this is modulino - call it directly).

=head1 SYNOPSIS

 ### Part 0 -> prepare the stage:
 # Step1: create a MySQL database named by date
 perl ./lib/CollectGenomes.pm --mode=create_db -ho localhost -d nr_2015_9_2 -p msandbox -u msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 # Step2: create a collection of directories inside a db directory (need to create manually - by date) to store all db files and preparation
 #also copies update_phylogeny file which is manually curated in doc directory
 perl ./lib/CollectGenomes.pm --mode=make_db_dirs -o /home/msestak/dropbox/Databases/db_02_09_2015/ -if /home/msestak/dropbox/Databases/db_29_07_15/doc/update_phylogeny_martin7.tsv

 ### Part I -> download genomes from Ensembl:
 # Step 1: download protists, fungi, metazoa and bacteria (21085)
 perl ./lib/CollectGenomes.pm --mode=ensembl_ftp --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_ftp/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 # Step2: download vertebrates
 #need to scrape HTML to get to taxids in order to download vertebrates from Ensembl (+78 = total 21163) downloaded 67 vertebrates + 2 (S.cerevisiae and C. elegans) + 27 PRE (but duplicates (real 11))
 perl ./lib/CollectGenomes.pm --mode=ensembl_vertebrates --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_vertebrates/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #copy ensembl proteomes to ensembl_all (7 min)
 time cp ./ensembl_ftp/* ./ensembl_all/
 cp -i ./ensembl_vertebrates/* ./ensembl_all/
 cp: overwrite `./ensembl_all/4932'? y   (S. cerevisiae)
 cp: overwrite `./ensembl_all/6239'? y   (C. elegans)

 
 ### Part II -> download genomes from NCBI:
 # Step1: download NCBI nr protein fasta file, gi_taxid_prot and taxdump
 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /blast/db/FASTA/ -rf nr.gz
 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf gi_taxid_prot.dmp.gz
 perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf taxdump.tar.gz
 #taxdmp is needed for names and nodes files (phylogeny information)
 [msestak@tiktaalik nr_raw]$ tar -xzvf taxdump.tar.gz
 [msestak@tiktaalik nr_raw]$ rm citations.dmp delnodes.dmp gc.prt merged.dmp gencode.dmp


 ### Part IIa -> download genomes from JGI:
 perl ./lib/CollectGenomes.pm --mode=jgi_download --names=names_raw_2015_9_3_new -tbl gold=gold_ver5 -o /home/msestak/dropbox/Databases/db_02_09_2015/data/xml/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 ### Part III -> load nr into database:
 # Step1: load gi_taxid_prot to connect gi from nr and ti from gi_taxid_prot
 perl ./lib/CollectGenomes.pm --mode=gi_taxid -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
 #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz has 223469419 lines!
 #File /home/msestak/gitdir/CollectGenomes/t/nr/gi_taxid_prot_TokuDB written with 223469419 lines!
 #Report: import inserted 223469419 rows in 3331 sec (67087 rows/sec)
 # Step2: load full nr NCBI database
 perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
 #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz has 70614921 lines!
 #File /home/msestak/gitdir/CollectGenomes/t/nr/nr_2015_9_3_TokuDB written with 211434339 lines!
 #Report: import inserted 211434339 rows! in 28969 sec (7298 rows/sec)
 
 ### Part IV -> set phylogeny for focal species:
 # Step1: load raw names and nodes and prune nodes of Viruses and other unwanted sequences
 perl ./lib/CollectGenomes.pm --mode=import_raw_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
 #Action: inserted 1987756 rows to names:names_dmp in 81 sec (24540 rows/sec)
 #PRUNING partI: excluded Phages, Viruses, Sythetic and Environmental samples while loading nodes_dmp
 perl ./lib/CollectGenomes.pm --mode=import_raw_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
 #Action: inserted 1124194 rows to nodes:nodes_dmp in 45 sec (24982 rows/s)
 
 # Step2: import tis of Ensembl genomes, count them and get a list of files for MakeTree
 perl ./lib/CollectGenomes.pm --mode=get_ensembl_genomes --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_all/ --tables names=names_raw_2015_9_3_new -o /home/msestak/dropbox/Databases/db_02_09_2015/doc/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -en=TokuDB
 #Action: update to ensembl_genomes updated 21163 rows!
 #Report: table ensembl_genomes has 21163 rows

 # Step3: run MakeTree to get modified phylogeny
 [msestak@tiktaalik db_02_09_2015]$ MakeTree -m ./data/nr_raw/names_raw_2015_9_3 -n ./data/nr_raw/nodes_raw_2015_9_3 -i ./doc/update_phylogeny_martin7.tsv -d 3 -s ./doc/ensembl -t 6072 | TreeIlustrator.pl
 Eumetazoa[6072]
 ├─Placozoa[10226]
 │ └─Trichoplax[10227]
 │   └─Trichoplax_adhaerens[10228]
 └─Cnidaria/Bilateria[1708696]
   ├─Cnidaria[6073]
   │ ├─Medusozoa[1708697]
   │ └─Anthozoa[6101]
   └─Bilateria[33213]
     ├─Deuterostomia[33511]
     └─Protostomia[33317]

 ---------------------------------------------
 Modified names and nodes file can be found in :
 ---------------------------------------------

 Nodes: ./data/nr_raw/nodes_raw_2015_9_3.new
 Names: ./data/nr_raw/names_raw_2015_9_3.new
 
 # Step4: import modified names and nodes to database
 perl ./lib/CollectGenomes.pm --mode=import_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
 perl ./lib/CollectGenomes.pm --mode=import_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB

 # Step5: create phylo tables for Other(28384 for pruning) and Species of interest (here 7955 Danio rerio)
 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 28384 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
 
 # Step6: PRUNING partII: delete rest of Other sequences (28384 most deleted in loading raw nodes - Synthetic)
 perl ./lib/CollectGenomes.pm -mode=del_virus_from_nr -tbl nr=gi_taxid_prot_TokuDB -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v

 # Step7: PRUNING partIII: delete all taxids that are present in gi_ti_prot_dmp table but not in updated nodes table
 #also delete all taxids which are not leaf nodes (species)
 perl ./lib/CollectGenomes.pm -mode=del_missing_ti -tbl nr=gi_taxid_prot_TokuDB -tbl nodes=nodes_raw_2015_9_3_new -tbl names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v
 #Report: deleted total of 2630957 rows in mode: genera

 ### Part V -> get genomes from nr base:
 # Step1: long running - JOIN of nr base and gi_taxid_prot table
 perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
 #Report: import inserted 204044303 rows in 25266 sec (8075 rows/sec)

 # Step2: COUNT all genomes by taxid
 perl ./lib/CollectGenomes.pm --mode=nr_genome_counts --tables nr=nr_ti_gi_fasta_TokuDB --tables names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
 #Action: import to nr_ti_gi_fasta_TokuDB_cnt inserted 455063 rows in 900 sec 
 #Action: update to nr_ti_gi_fasta_TokuDB_cnt updated 455063 rows!
 
 ### Part VI -> combine nr genomes with Ensembl genomes and print them out:
 # Step1:delete genomes from nr_cnt table that are present in ensembl_genomes (downloaded from Ensembl)
 #it also deletes genomes smaller than 2000 sequences
 #it also deletes all genomes having 'group' in name
 #prints report at end
 perl ./lib/CollectGenomes.pm --mode=get_missing_genomes --tables nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
 #Action: table nr_ti_gi_fasta_TokuDB_cnt deleted 21139 rows!
 #Action: table nr_ti_gi_fasta_TokuDB_cnt deleted 427679 rows!
 #Action: deleted 20 groups from table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 6225 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 4854 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 3533 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 2589 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1981 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1562 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1296 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1087 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 959 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 618 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 468 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 373 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 276 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt

 # Step2: delete genomes with species and strain genomes overlaping (nr only)
 perl ./lib/CollectGenomes.pm --mode=del_nr_genomes -tbl nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #Report: found 6096 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 4750 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 3446 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 2510 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1907 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1494 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1232 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 1027 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 900 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 565 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 417 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 327 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 #Report: found 170 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
 
 # Step3: imports nr and existing genomes
 perl ./lib/CollectGenomes.pm --mode=del_total_genomes -tbl nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -en=TokuDB
 #Action: import inserted 6096 rows!
 #Action: import inserted 21163 rows!
 #Action: deleted 2 hybrid species from ti_full_list
 #Report: found 26265 genomes in table:ti_full_list
 
 # Step4: extract nr genomes after filtering
 perl ./lib/CollectGenomes.pm --mode=print_nr_genomes -tbl ti_full_list=ti_full_list -tbl nr_ti_fasta=nr_ti_gi_fasta_TokuDB -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_genomes/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #printed 5162 genomes

 # Step5: remove genomes from jgi that are found in nr or ensembl (to jgi_clean directory)
 perl ./lib/CollectGenomes.pm --mode=copy_jgi_genomes -tbl ti_full_list=ti_full_list -tbl names=names_raw_2015_9_3_new --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/jgi/ --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/jgi_clean/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 # Action: update to ti_full_list updated 221 rows!
 # Report: found 219 JGI genomes in table:ti_full_list

 # Step6: copy genomes (external) from previous database not in this one
 perl ./lib/CollectGenomes.pm --mode=copy_external_genomes -tbl ti_full_list=ti_full_list -tbl names=names_raw_2015_9_3_new --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/external/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 # Action: update to ti_full_list updated 168 rows!
 # Report: found 167 external genomes in table:ti_full_list

 # Step7: delete duplicates from final database
 perl ./lib/CollectGenomes.pm --mode=del_species_with_strain -tbl ti_full_list=ti_full_list -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #Action: deleted 1 hybrid species from ti_full_list
 #10 genomes deleted
 #Report: found 26589 genomes in table:ti_full_list

 # Step8: merge jgi, nr, external, ensembl genomes to all:
 # it deletes genomes with taxid < 100 because of Centos6 kernel Boost issue in MakePhyloDb
 perl ./lib/CollectGenomes.pm --mode=merge_existing_genomes -o /home/msestak/dropbox/Databases/db_02_09_2015/data/all/ -tbl ti_full_list=ti_full_list -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #Copied 26586 genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all (40.2 GB)
 #Copied 214 JGI genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
 #Copied 5147 NCBI genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
 #Copied 145 external genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
 #Copied 21067 Ensembl genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all

 ### Part VII -> prepare and run cd-hit
 # Step1: run MakePhyloDb to get pgi||ti|pi|| identifiers (7h) and .ff extension
 [msestak@tiktaalik data]$ cp ./all_raw/ ./all_sync/
 [msestak@tiktaalik data]$ MakePhyloDb -d ./all_sync/

 # Step2: remove .ff from genomes that are not leaf nodes
 # and put nodes on 0 that are behind genome node
 DbSync.pl -d ./all_sync/ -n ./nr_raw/nodes.dmp.fmt.new
 mv ./all_sync/*.ff ./all_ff/
 #to update statistics
 [msestak@tiktaalik data]$ mv ./all_sync/*.ff ./all_ff/
 [msestak@tiktaalik data]$ ls ./all_sync/ | wc -l
 #1331
 [msestak@tiktaalik data]$ ls ./all_ff/ | wc -l
 #25244
 #copy info files to update them
 [msestak@tiktaalik data]$ cp ./all_sync/info.* ./all_ff/
 #update info files for Phylostrat
 [msestak@tiktaalik data]$ MakePhyloDb -d ./all_ff/
 [msestak@tiktaalik data]$ cat ./all_ff/info.paf 
 #2015-9-24.13:45:35 :Database Created On:
 #25244 :Number Of Genomes:
 #38538642861 :Database Size:
 #37564616819 :Effective Database Size:
 [msestak@tiktaalik data]$ cat ./all_sync/info.paf 
 #2015-9-22.18:47:19 :Database Created On:
 #26573 :Number Of Genomes:
 #41516744334 :Database Size:
 #40475950304 :Effective Database Size:

 # Step3: analyze database
 [msestak@tiktaalik data]$ AnalysePhyloDb -d ./all_ff/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_25244_genomes_danio
 [msestak@tiktaalik data]$ grep "<ps>" analyze_25244_genomes_danio > analyze_25244_genomes_danio.ps
 [msestak@tiktaalik data]$ cat analyze_25244_genomes_danio.ps 
 #<ps>	1	19665	131567
 #<ps>	2	266	2759
 #<ps>	3	13	1708629
 #<ps>	4	1	1708631
 #<ps>	5	787	33154
 #<ps>	6	2	1708671
 #<ps>	7	1	1708672
 #<ps>	8	2	1708673
 #<ps>	9	7	33208
 #<ps>	10	1	6072
 #<ps>	11	8	1708696
 #<ps>	12	133	33213
 #<ps>	13	2	33511
 #<ps>	14	1	7711
 #<ps>	15	3	1708690
 #<ps>	16	1	7742
 #<ps>	17	1	7776
 #<ps>	18	0	117570
 #<ps>	19	167	117571
 #<ps>	20	0	7898
 #<ps>	21	0	186623
 #<ps>	22	1	41665
 #<ps>	23	1	32443
 #<ps>	24	1	1489341
 #<ps>	25	22	186625
 #<ps>	26	1	186634
 #<ps>	27	0	32519
 #<ps>	28	2	186626
 #<ps>	29	0	186627
 #<ps>	30	0	7952
 #<ps>	31	0	30727
 #<ps>	32	2	7953
 #<ps>	33	0	7954
 #<ps>	34	1	7955
 [msestak@tiktaalik data]$ grep -P "^\d+\t" analyze_25244_genomes_danio > analyze_25244_genomes_danio.genomes

 # Step 3b: remove genomes found in all_ff directory but not found in AnalysePhyloDb file (not found in nodes.dmp.fmt.new.sync because at 0) -> deleted before
 perl ./lib/CollectGenomes.pm --mode=del_after_analyze -i /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff/ -if /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_all_ff -o /home/msestak/dropbox/Databases/db_02_09_2015/data/all_sync/
 #Report: found 25244 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff
 #Report: found 25224 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_all_ff
 #Report: removed 20 genomes out of /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff to /home/msestak/dropbox/Databases/db_02_09_2015/data/all_sync

 # Step4: partition genomes per phylostrata for cdhit
 perl ./lib/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff/ --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ -tbl phylo=phylo_7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #Report: 26 phylostrata:{ps1 ps2 ps3 ps4 ps5 ps6 ps7 ps8 ps9 ps10 ps11 ps12 ps13 ps14 ps15 ps16 ps17 ps19 ps22 ps23 ps24 ps25 ps26 ps28 ps32 ps34}
 #Action: dir /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2 removed and cleaned
 #Action: concatenated 23757 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps1.fa
 #Action: concatenated 277 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps2.fa
 #Action: concatenated 13 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps3.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4.fa
 #Report: ps4 has 1 genomes and is excluded for cdhit
 #Action: File /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4.fa renamed to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4
 #Action: concatenated 806 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps5.fa
 #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps6.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps7.fa
 #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps8.fa
 #Action: concatenated 7 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps9.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps10.fa
 #Action: concatenated 8 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps11.fa
 #Action: concatenated 134 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps12.fa
 #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps13.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps14.fa
 #Action: concatenated 3 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps15.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps16.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps17.fa
 #Action: concatenated 167 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps19.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps22.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps23.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps24.fa
 #Action: concatenated 22 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps25.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps26.fa
 #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps28.fa
 #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps32.fa
 #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps34.fa
 
 # Step5: run cdhit based on cd_hit_cmds file
 [msestak@tiktaalik CollectGenomes]$ perl ./lib/CollectGenomes.pm --mode=run_cdhit --if=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/cd_hit_cmds_ps1 --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/
 #run ps1 separately
 [msestak@cambrian-0-0 CollectGenomes]$ perl ./lib/CollectGenomes.pm --mode=run_cdhit --if=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/

 # Step5: combine all cdhit files into one db and replace J to * for BLAST
 perl ./lib/CollectGenomes.pm --mode=cdhit_merge -i /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/ -of /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/blast_db -o /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
 #Report: printed 43923562 fasta records to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/blast_db (18.1 GB)
 #Report: printed 22290 genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
 
 # Step6: add some additional genomes to database
 perl ./lib/CollectGenomes.pm --mode=manual_add_fasta -if ./cdhit/V2.0.CommonC.pfasta -o ./cdhit/ -t 7962
 #Report: transformed /msestak/gitdir/CollectGenomes/cdhit/V2.0.CommonC.pfasta to /msestak/gitdir/CollectGenomes/cdhit/7962 (46609 rows) with BLAST_format = true

 # Step7: rum MakePhyloDb and AnalysePhyloDb again to get accurate info after cdhit
 [msestak@tiktaalik data]$ MakePhyloDb -d ./cdhit_large/extracted/
 [msestak@tiktaalik data]$ AnalysePhyloDb -d ./cdhit_large/extracted/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_cdhit_large

 [msestak@tiktaalik data]$ MakePhyloDb -d ./cdhit_large/extracted/
 [msestak@tiktaalik data]$ AnalysePhyloDb -d ./cdhit_large/extracted/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_cdhit_large
 [msestak@tiktaalik data]$ grep -P "^\d+\t" analyze_cdhit_large > analyze_cdhit_large.genomes
 [msestak@tiktaalik data]$ wc -l analyze_cdhit_large.genomes 
 #22290 analyze_cdhit_large.genomes
 [msestak@tiktaalik data]$ mkdir ./cdhit_large/surplus
 perl ./lib/CollectGenomes.pm --mode=del_after_analyze -i /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted/ -if /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_cdhit_large -o /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/surplus/
 #Report: found 22290 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
 #Report: found 22290 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_cdhit_large
 #Report: removed 0 genomes out of /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/surplus



 ### Part VIII -> prepare for BLAST
 # Step1: get longest splicing var
 [msestak@tiktaalik in]$ SplicVar.pl -f Danio_rerio.GRCz10.pep.all.fa -l L > danio_splicvar 
 [msestak@tiktaalik in]$ grep -c ">" Danio_rerio.GRCz10.pep.all.fa 
 #44487
 [msestak@tiktaalik in]$ grep -c ">" danio_splicvar
 #25638
 perl ./lib/FastaSplit.pm -if /msestak/workdir/danio_dev_stages_phylo/in/dr_splicvar -name dr -o /msestak/workdir/danio_dev_stages_phylo/in/in_chunks_dr -n 50 -s 7000 -a
 #Num of seq: 25638
 #Num of chunks: 50
 #Num of seq in chunk: 512
 #Num of seq left without chunk: 38
 #Larger than 7000 {7 seq}: 27765 22190 9786 8864 8710 8697 7035

 ### Part IX -> backup a database
 /home/msestak/gitdir/CollectGenomes/lib/CollectGenomes.pm --mode=mysqldump -d blastdb -o . -u msandbox --password=msandbox --port=5622 --socket=/tmp/mysql_sandbox5622.sock -v -v

=head1 DESCRIPTION

CollectGenomes is modulino that downloads genomes (actually proteomes) from Ensembl FTP servers. It names them by tax_id.
It can also download NCBI nr database and extract genomes from it (requires MySQL).
It runs clustering with cd-hit and builds a BLAST database per species analyzed.

To use different functionality use specific modes.
Possible modes:

 create_db                     => \&create_db,
 ftp                           => \&ftp_robust,
 extract_nr                    => \&extract_nr,
 extract_and_load_nr           => \&extract_and_load_nr,
 gi_taxid                      => \&extract_and_load_gi_taxid,
 ti_gi_fasta                   => \&ti_gi_fasta,
 get_ensembl_genomes               => \&get_ensembl_genomes,
 import_names                  => \&import_names,
 import_nodes                  => \&import_nodes,
 get_missing_genomes           => \&get_missing_genomes,
 del_nr_genomes          => \&del_nr_genomes,
 del_total_genomes           => \&del_total_genomes,
 print_nr_genomes              => \&print_nr_genomes,
 merge_existing_genomes         => \&merge_existing_genomes,
 ensembl_vertebrates           => \&ensembl_vertebrates,
 ensembl_ftp                   => \&ensembl_ftp,
 prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
 run_cdhit                     => \&run_cdhit,

For help write:

 perl CollectGenomes.pm -h
 perl CollectGenomes.pm -m

=head1 EXAMPLE 02.09.2015 on tiktaalik

 ALTERNATIVE with Deep:
 perl ./lib/CollectGenomes.pm --mode=create_db -ho localhost -d nr_2015_9_2 -p msandbox -u msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock
 perl ./lib/CollectGenomes.pm --mode=gi_taxid -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
 #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz has 223469419 lines!
 #File /home/msestak/gitdir/CollectGenomes/t/nr/gi_taxid_prot_Deep written with 223469419 lines!
 #import inserted 223469419 rows! in 3381 sec (66095 rows/sec)
 
 perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
 #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz has 70614921 lines!
 #File /home/msestak/gitdir/CollectGenomes/t/nr/nr_2015_9_3_Deep written with 211434339 lines!
 #import inserted 211434339 rows! in 20447 sec (10340 rows/sec)
 #copy missing tables to other MySQL server
 mysqldump nr_2015_9_2 species_ensembl_divisions -u msandbox -p'msandbox' --single-transaction --port=5625 --socket=/tmp/mysql_sandbox5625.sock | mysql -D nr_2015_9_2 -u msandbox -p'msandbox' --port=5626 --socket=/tmp/mysql_sandbox5626.sock
 perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep

 ### Part IV -> set phylogeny for focal species:

 perl ./lib/CollectGenomes.pm --mode=import_raw_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
 #Action: inserted 1987756 rows to names:names_dmp in 69 sec (28808 rows/sec)
 perl ./lib/CollectGenomes.pm --mode=import_raw_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
 #Action: inserted 1124194 rows to nodes:nodes_dmp in 31 sec (36264 rows/s)
 perl ./lib/CollectGenomes.pm --mode=import_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
 perl ./lib/CollectGenomes.pm --mode=import_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep

 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v --engine=Deep
 perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 28384 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v --engine=Deep


 #PRUNING partII: delete rest of Other sequences (most deleted in loading raw nodes - Synthetic)
 perl ./lib/CollectGenomes.pm -mode=del_virus_from_nr -tbl nr=gi_taxid_prot_Deep -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v
 #PRUNING partIII: delete all taxids that are present in gi_ti_prot_dmp table but not in updated nodes table
 #also delete all taxids which are not leaf nodes (species)
 perl ./lib/CollectGenomes.pm -mode=del_missing_ti -tbl nr=gi_taxid_prot_Deep -tbl nodes=nodes_raw_2015_9_3_new -tbl names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v
 #Report: deleted total of 2630957 rows in mode: genera
 
 ### Part V -> get genomes from nr base:
 perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
 #Report: import inserted 204044303 rows in 5312 sec (38411 rows/sec)

 #perl ./lib/CollectGenomes.pm --mode=nr_genome_counts --tables nr=nr_ti_gi_fasta_Deep --tables names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
 #Action: import to nr_ti_gi_fasta_Deep_cnt inserted 455063 rows in 200 sec
 #Action: update to nr_ti_gi_fasta_Deep_cnt updated 455063 rows!
 
 ### Part VI -> combine nr genomes with Ensembl genomes and print them out:
 #deletes genomes from nr_cnt table that are present in ensembl_genomes (downloaded from Ensembl)
 #it also deletes genoes smaller than 2000 sequences
 perl ./lib/CollectGenomes.pm --mode=get_missing_genomes --tables nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 #Action: table nr_ti_gi_fasta_Deep_cnt deleted 21139 rows!
 #Action: table nr_ti_gi_fasta_Deep_cnt deleted 427679 rows!
 #Report: found 6245 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 4870 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 3543 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 2598 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1990 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1571 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1304 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1093 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 965 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 620 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 469 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 374 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 26 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 
 perl ./lib/CollectGenomes.pm --mode=del_nr_genomes -tbl nr_cnt=nr_ti_gi_fasta_Deep_cnt -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock
 #deletes genomes with species and strain genomes overlaping (only nr)
 #Report: found 5928 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 4600 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 3325 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 2404 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1805 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1396 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 1139 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 939 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 813 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 501 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 365 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 285 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_Deep_cnt
 #Report: found 3 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_Deep_cnt

 perl ./lib/CollectGenomes.pm --mode=del_total_genomes -tbl nr_cnt=nr_ti_gi_fasta_Deep_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -en=Deep
 #imports nr and existing genomes
 #deletes hybrid genomes
 #Action: import inserted 5928 rows!
 #Action: import inserted 21163 rows!
 #Action: deleted 2 hybrid species from ti_full_list
 #Report: found 25063 genomes in table:ti_full_list

 #extract nr genomes after filtering
 perl ./lib/CollectGenomes.pm --mode=print_nr_genomes -tbl ti_full_list=ti_full_list -tbl nr_ti_fasta=nr_ti_gi_fasta_Deep -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_genomes/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock


=head1 LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Martin Sebastijan Šestak E<lt>msestak@irb.hrE<gt>

=cut

