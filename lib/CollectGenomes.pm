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

our $VERSION = "0.01";

our @EXPORT_OK = qw{
	main
    init_logging
	get_parameters_from_cmd
	dbi_connect
	create_database
	capture_output
	ftp_robust
	extract_nr
	load_nr
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
	ensembl_vertebrates
	ensembl_ftp
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
        $IN = path($INFILE)->absolute->canonpath;
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












1;
__END__

=encoding utf-8

=head1 NAME

CollectGenomes - Downloads genomes from Ensembl FTP (and NCBI nr db) and builds BLAST database (this is modulino - call it directly).

=head1 SYNOPSIS

 perl ./bin/CollectGenomes.pm --mode=create_db -i . -ho localhost -d nr -p msandbox -u msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 C:\workdir_doma\collect_genomes_to_database\bin>perl CollectGenomes.pm --mode=ftp_robust -o . -rh ftp.ncbi.nih.gov -rd /blast/db/FASTA/ -rf nr.gz
 C:\workdir_doma\collect_genomes_to_database\bin>perl CollectGenomes.pm --mode=ftp_robust -o . -rd /pub/taxonomy/ -rf gi_taxid_prot.dmp.gz
 C:\workdir_doma\collect_genomes_to_database\bin>perl CollectGenomes.pm --mode=ftp_robust -o . -rd /blast/db/FASTA/ -rf nr.gz
 C:\workdir_doma\collect_genomes_to_database\bin>perl CollectGenomes.pm --mode=ftp_robust -o . -rd /pub/taxonomy/ -rf taxdump.tar.gz

 NOT USED:perl ./bin/CollectGenomes.pm --mode=extract_nr -i /home/msestak/db_new/nr_19_06_2015/nr.gz -o /home/msestak/db_new/nr_19_06_2015/

 perl ./bin/CollectGenomes.pm --mode=extract_and_load_nr -i /home/msestak/db_new/nr_19_06_2015/nr.gz -o /home/msestak/db_new/nr_19_06_2015/ -ho localhost -u msandbox -p msandbox -d nr --port=5622 --socket=/tmp/mysql_sandbox5622.sock --engine=TokuDB

 perl ./bin/CollectGenomes.pm --mode=gi_taxid -i ./t/gi_taxid_prot1000.gz -o /home/msestak/db_new/nr_19_06_2015/ -ho localhost -u msandbox -p msandbox -d nr --port=5622 --socket=/tmp/mysql_sandbox5622.sock --engine=Deep

 perl ./bin/CollectGenomes.pm --mode=ti_gi_fasta  -o . -d nr -ho localhost -u msandbox -p msandbox --port=5624 --socket=/tmp/mysql_sandbox5624.sock --engine=Deep

 perl ./bin/CollectGenomes.pm --mode=import_names -i ./t_eukarya/names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=import_nodes -i ./t_eukarya/nodes_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 or
 perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl blastdb_analysis.pl -mode=call_phylo -no nodes_martin7 -t 2759 -org eu --proc=proc_create_phylo16278 -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=get_existing_ti --in=./t_eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=get_missing_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=delete_extra_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=delete_full_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=print_nr_genomes --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=copy_existing_genomes --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya_old/  --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

 perl ./bin/CollectGenomes.pm --mode=ensembl_vertebrates --out=./ftp_ensembl/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=ensembl_ftp --out=./data_in/ftp_ensembl/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=./data_in/t_eukarya/ --out=./data_out/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
 perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_29_07_15/data/archaea/ --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


 perl ./bin/CollectGenomes.pm --mode=run_cdhit --in=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock -v



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

Copyright (C) mocnii Martin Sebastijan Šestak

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

mocnii E<lt>msestak@irb.hrE<gt>

=cut

