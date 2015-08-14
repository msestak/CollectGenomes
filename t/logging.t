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
my @subs = qw( main init_logging );

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

done_testing();



