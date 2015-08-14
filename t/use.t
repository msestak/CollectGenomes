#!/usr/bin/env perl
use strict;
use warnings;

use Test::More;

my $module = 'CollectGenomes';
my @subs = qw( main init_logging get_parameters_from_cmd dbi_connect create_database );

use_ok( $module, @subs);

foreach my $sub (@subs) {
    can_ok( $module, $sub);
}

done_testing();

