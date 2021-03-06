#!/usr/bin/env perl
use strict;
use warnings;

use Test::More;

my $module = 'CollectGenomes';
my @subs = qw( main init_logging get_parameters_from_cmd dbi_connect create_db 
  ensembl_ftp ensembl_ftp_vertebrates ftp_robust extract_and_load_nr extract_and_load_gi_taxid ti_gi_fasta download_from_stats);

use_ok( $module, @subs);

foreach my $sub (@subs) {
    can_ok( $module, $sub);
}

done_testing();

