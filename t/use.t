#!/usr/bin/env perl
use strict;
use warnings;

use Test::More tests => 3;

my @subs = qw( main init_logging );

use_ok( 'CollectGenomes', @subs);

can_ok( __PACKAGE__, 'main');
can_ok( __PACKAGE__, 'init_logging' );


