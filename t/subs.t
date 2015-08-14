#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;


use FindBin;
#use lib "$FindBin::Bin/../lib";
use Test::More;
use Test::Output qw(:functions);
use Capture::Tiny qw/capture/;
use CollectGenomes qw{
	main
    init_logging
	get_parameters_from_cmd
	dbi_connect
	create_database
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

my $module = 'CollectGenomes';
my @subs = qw( main init_logging get_parameters_from_cmd dbi_connect create_database );

use_ok( $module, @subs);

my $cmd = qq|perl $FindBin::Bin/../lib/CollectGenomes.pm -h|;
my ( $stdout, $stderr, $exit ) = capture {
    system($cmd);
};
END {say 'STDOUT  is: ', "$stdout", "\n", 'STDERR   is: ', "$stderr", "\n", 'EXIT    is: ', "$exit";}


##start
#init_logging();
#my $log = Log::Log4perl::get_logger("main");
#
##init_logging
#	#stderr_is( sub{ init_logging() }, qr/INVOCATION/, 'testing init_logging (bare)' );
#
#	my ($param_href) = get_parameters_from_cmd();
#	say Dumper($param_href);

done_testing();
