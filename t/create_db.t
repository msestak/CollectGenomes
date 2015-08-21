#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;

use FindBin;
use Log::Log4perl;
use Test::Log::Log4perl;
use Test::More;
use CollectGenomes ('init_logging');

my $module = 'CollectGenomes';
my @subs = qw( create_database create_table dbi_connect import_names );

use_ok( $module, @subs);

#SETUP TEST SERVER BEFORE (DROP DATABASE deletes all)
#needed to init logging
init_logging();

# get the loggers
my $log  = Log::Log4perl->get_logger("main");
my $tlog = Test::Log::Log4perl->get_logger("main");

#testing create_database() log mesages
my $param_href = {
          'VERBOSE' => 1,
          'PORT' => 5624,
          'USER' => 'msandbox',
          'DATABASE' => 'nr',
          'SOCKET' => '/tmp/mysql_sandbox5624.sock',
          'HOST' => 'localhost',
          'PASSWORD' => 'msandbox',
          'MODE' => [
                      'create_db'
                    ],
          'CHARSET' => 'ascii',
        };
Test::Log::Log4perl->start();
$tlog->trace(qr/Report: connected to DBI:/);
$tlog->info(qr/nr database creation with CHARSET ascii/);
$tlog->debug(qr/Database nr dropped successfully!/);
$tlog->debug(qr/Database nr created successfully!/);
create_database($param_href);
Test::Log::Log4perl->end(qq|Report: sub create_database() logging works|);


#testing import_names() log mesages
$param_href = ();
$param_href = {
          'USER' => 'msandbox',
          'ENGINE' => 'InnoDB',
          'SOCKET' => '/tmp/mysql_sandbox5624.sock',
          'VERBOSE' => 1,
          'MODE' => [
                      'import_names'
                    ],
          'PORT' => 5624,
          'DATABASE' => 'nr',
          'HOST' => 'localhost',
          'PASSWORD' => 'msandbox'
        };
$param_href->{INFILE} = "$FindBin::Bin/../nr/names_martin7";
Test::Log::Log4perl->start(ignore_priority => "trace" );   #variable number of trace messages imposible to test
$tlog->info(qr/---------->Importing names/);
$tlog->info(qr/Action.+?dropped successfully!/);
$tlog->info(qr/Action.+?created successfully!/);
$tlog->info(qr/Report: import inserted/);
$tlog->debug(qr/Report: table.+?loaded successfully!/);
import_names($param_href);
Test::Log::Log4perl->end(qq|Report: sub import_names() logging works|);









done_testing();

