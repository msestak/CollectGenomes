#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;


use FindBin;
use Test::More;
use Capture::Tiny qw/capture/;
use CollectGenomes qw{
	main
    init_logging
	get_parameters_from_cmd
	};

my $module = 'CollectGenomes';
my @subs = qw( main init_logging get_parameters_from_cmd dbi_connect create_db );

use_ok( $module, @subs);

#testing main()
my $cmd_help = qq|perl $FindBin::Bin/../lib/CollectGenomes.pm -h|;
my ( $stdout, $stderr, $exit ) = capture {
    system($cmd_help);
};
#END {say 'STDOUT  is: ', "$stdout", "\n", 'STDERR   is: ', "$stderr", "\n", 'EXIT    is: ', "$exit";}

like ($stdout, qr/Usage:/, 'stdout calling module with help -h');
like ($stderr, qr/This is start of logging/, 'stderr calling module with help -h');

my $cmd_man = qq|perl $FindBin::Bin/../lib/CollectGenomes.pm -m|;
my ( $stdout_man, $stderr_man, $exit_man ) = capture {
    system($cmd_man);
};
#END {say 'STDOUT  is: ', "$stdout_man", "\n", 'STDERR   is: ', "$stderr_man", "\n", 'EXIT    is: ', "$exit_man";}

like ($stdout_man, qr/SYNOPSIS/, 'stdout calling module with man -m');
like ($stderr_man, qr/This is start of logging/, 'stderr calling module with man -m');

#testing get_parameters_from_cmd()
my $cmd_mode = qq|perl $FindBin::Bin/../lib/CollectGenomes.pm --mode=create_db|;
my ( $stdout_m, $stderr_m, $exit_m ) = capture {
    system($cmd_mode);
};
#END {say 'STDOUT  is: ', "$stdout_m", "\n", 'STDERR   is: ', "$stderr_m", "\n", 'EXIT    is: ', "$exit_m";}

like ($stdout_m, qr//, 'stdout empty when calling module with --mode=create_db');
like ($stderr_m, qr/RUNNING ACTION for mode: create_db/, 'stderr calling module with --mode=create_db');

done_testing();
