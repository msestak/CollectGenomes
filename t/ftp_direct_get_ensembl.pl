#!/usr/bin/env perl

use strict;
use warnings;
use Net::FTP;
use 5.010001;
use Path::Tiny;

my $get_loc = 'ftp://ftp.ensemblgenomes.org/pub/release-34/protists/fasta/albugo_laibachii/pep/Albugo_laibachii.ENA1.pep.all.fa.gz';
my $division = 'protists';
my $REMOTE_HOST = 'ftp.ensemblgenomes.org';

#FIRST:connect to ftp to download info about genomes
my $ftp = Net::FTP->new($REMOTE_HOST, Debug => 1) or die "Action: Can't connect to $REMOTE_HOST: $@";
$ftp->login("anonymous",'msestak@irb.hr')         or die "Action: Can't login ", $ftp->message;
#$ftp->binary()                                    or die( "Opening binary mode data connection failed for $_: $@" );
#$ftp->pasv()                                      or die( "Opening passive mode data connection failed for $_: $@" );
#my $remote_path = path('pub', $division, 'current');
##$ftp->cwd($remote_path)     
#my $local_file = '/msestak/workdir/nr_22_03_2017/Albugo_laibachii.ENA1.pep.all.fa.gz';
#open my $local_fh, ">", $local_file or die( "Can't write to $local_file:$!" );
##$ftp->get($get_loc, $local_fh) and print "Action: download to $local_file";
#$ftp->get($get_loc) and print "Action: download to $local_file";

my $dir = '/pub/release-34/bacteria/fasta/bacteria_27_collection';
$ftp->cwd($dir) or $ftp->message;
my $species_dir = 'salmonella_enterica_subsp_enterica_serovar_agona_str_40_e_08';
	my $spec_path = path($species_dir, 'pep');
	$ftp->cwd($spec_path) or say "Can't change working directory to $spec_path", $ftp->message;

