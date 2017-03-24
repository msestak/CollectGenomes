#!/usr/bin/env perl

use strict;
use warnings;
use Net::FTP;
use 5.010001;
use Path::Tiny;
use Data::Dumper;

my $OUT         = path('/msestak/workdir/nr_22_03_2017/');
my $division    = 'bacteria';
my $REMOTE_HOST = 'ftp.ensemblgenomes.org';

# connect to ftp
my $ftp = Net::FTP->new( $REMOTE_HOST, Debug => 0 ) or die("Action: Can't connect to $REMOTE_HOST: $@");
$ftp->login( "anonymous", 'msestak@irb.hr' ) or die( "Action: Can't login ", $ftp->message );
$ftp->binary() or die("Opening binary mode data connection failed for $_: $@");
$ftp->pasv()   or die("Opening passive mode data connection failed for $_: $@");
my $remote_path = path( 'pub', 'current', $division, 'fasta' );
$ftp->cwd($remote_path) or die( "Can't change working directory ", $ftp->message );
my $pwd = $ftp->pwd;
say "$pwd";
my @collections = $ftp->ls;
#print Dumper( \@collections );

# list bacterial collections
foreach my $collection (@collections) {

    #	if ($collection ne 'bacteria_27_collection') {
    #		next;
    #	}
    #else {
    # list species inside collections
    $ftp->cwd($collection);
    my $pwd_coll = $ftp->pwd;
    say "$pwd_coll";
    my @species_dir = $ftp->ls;
	#print Dumper( \@species_dir );

    # list files inside salmonella_enterica_subsp_enterica
    foreach my $species (@species_dir) {
        if ( $species =~ m{salmonella_enterica_subsp_enterica} ) {
            my $sp_path = path( $species, 'pep' );
            $ftp->cwd($sp_path);
            my $pwd_sp = $ftp->pwd;
            say "$pwd_sp";
            my @pep_listing = $ftp->ls;
            print Dumper( \@pep_listing );

            #get fasta file inside
          FILE:
            foreach my $proteome (@pep_listing) {
                next FILE unless $proteome =~ m/pep.all.fa.gz\z/;
                my $local_file = path( $OUT, $proteome );

                #print stats and go up 2 dirs
                my $stat_file = path($OUT);
                $stat_file = path( $stat_file, "statistics_ensembl_all$$.txt" )->canonpath;
                if ( -f $stat_file ) {
                    warn("Action: STAT file:$stat_file already exists: appending");
                }
                open my $stat_fh, '>>', $stat_file or die "can't open file: $!";
                print {$stat_fh} path( $REMOTE_HOST, $ftp->pwd(), $proteome ), "\t$REMOTE_HOST\t", $ftp->pwd(),
                  "\t$proteome\n";
            }    #foreach FILE end
        }
    }

    #}
}

#$ftp->cwd($dir) or $ftp->message;
#my $species_dir = 'salmonella_enterica_subsp_enterica_serovar_agona_str_40_e_08';
#	my $spec_path = path($species_dir, 'pep');
#	$ftp->cwd($spec_path) or say "Can't change working directory to $spec_path", $ftp->message;

