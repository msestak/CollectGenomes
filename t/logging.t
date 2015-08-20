#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;

use Test::Log::Log4perl;
use FindBin;
use Test::More;
use Capture::Tiny qw/capture/;
use CollectGenomes qw{
	main
    init_logging
	};

my $module = 'CollectGenomes';
my @subs = qw( main init_logging capture_output get_parameters_from_cmd dbi_connect create_database);

use_ok( $module, @subs);

#testing init_logging()
init_logging();

# get the loggers
my $log  = Log::Log4perl->get_logger("main");
my $tlog = Test::Log::Log4perl->get_logger("main");
 
#testing trace level
Test::Log::Log4perl->start();
$tlog->trace("This is a test");
$log->trace("This is a test");
Test::Log::Log4perl->end(qq|Report: trace level works|);

#testing debug level
Test::Log::Log4perl->start();
$tlog->debug("This is a test");
$log->debug("This is a test");
Test::Log::Log4perl->end(qq|Report: debug level works|);

#testing info level
Test::Log::Log4perl->start();
$tlog->info("This is a test");
$log->info("This is a test");
Test::Log::Log4perl->end(qq|Report: info level works|);

#testing warn level
Test::Log::Log4perl->start();
$tlog->warn("This is a test");
$log->warn("This is a test");
Test::Log::Log4perl->end(qq|Report: warn level works|);

#testing error level
Test::Log::Log4perl->start();
$tlog->error("This is a test");
$log->error("This is a test");
Test::Log::Log4perl->end(qq|Report: error level works|);

#testing fatal level
Test::Log::Log4perl->start();
$tlog->fatal("This is a test");
$log->fatal("This is a test");
Test::Log::Log4perl->end(qq|Report: fatal level works|);


#testing main() log mesages
Test::Log::Log4perl->start();
$tlog->info(qr/This is start/);
$tlog->trace(qr/My \@ARGV:/);
$tlog->trace(qr/before/);
$tlog->trace(qr/after/);
$tlog->fatal(qr/command line/);

main();

Test::Log::Log4perl->end(qq|Report: main sub logging works|);


#testing capture_output() log mesages
my $cmd = q{ls -lh};
Test::Log::Log4perl->start();
$tlog->info(qr/COMMAND is:/);
my ($stdout, $stderr, $exit) = capture_output( $cmd );
Test::Log::Log4perl->end(qq|Report: sub capture_output logging works (vebose = off)|);

my $VERBOSE = 1;
my $param_href;
$param_href->{VERBOSE} = 1;
Test::Log::Log4perl->start();
$tlog->info(qr/COMMAND is:/);
$tlog->trace(qr/STDOUT is:/);
my ($stdout_on, $stderr_on, $exit_on) = capture_output( $cmd, $param_href );
Test::Log::Log4perl->end(qq|Report: sub capture_output logging works (vebose = on)|);


#testing get_parameters_from_cmd log mesages
Test::Log::Log4perl->start();
$tlog->trace(qr/My \@ARGV:/);
$tlog->trace(qr/before/);
$tlog->trace(qr/after/);
$tlog->fatal(qr/command line/);
$param_href = '';
($param_href) = get_parameters_from_cmd();
Test::Log::Log4perl->end(qq|Report: sub get_parameters_from_cmd logging works|);


done_testing();



