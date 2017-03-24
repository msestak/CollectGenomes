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
#print Dumper(\@collections);

# list bacterial collections
foreach my $collection (@collections) {

    # list species inside collections
    $ftp->cwd($collection);
    my $pwd_coll = $ftp->pwd;
    say "$pwd_coll";
    my @species_dir = $ftp->ls;
    #print Dumper( \@species_dir );

    # list species dirs inside collection
    foreach my $species (@species_dir) {
        my $sp_path = path( $species, 'pep' );
        $ftp->cwd($sp_path);
        my $pwd_sp = $ftp->pwd;
        say "$pwd_sp";
        my @pep_listing = $ftp->ls;
        #print Dumper( \@pep_listing );

        #get fasta file inside
      FILE:
        foreach my $proteome (@pep_listing) {
            next FILE unless $proteome =~ m/pep.all.fa.gz\z/;
            my $local_file = path( $OUT, $proteome );

            #print stats and go up 2 dirs
            my $stat_file = path( $OUT, "statistics_ensembl_all$$.txt" )->canonpath;
            open my $stat_fh, '>>', $stat_file or die "can't open file: $!";
            print {$stat_fh} path( $REMOTE_HOST, $ftp->pwd(), $proteome ), "\t$REMOTE_HOST\t", $ftp->pwd(),
              "\t$proteome\n";
        }    #foreach FILE end

        #go 2 dirs up to $collection (cdup = current dir up)
        $ftp->cdup();
        $ftp->cdup();
    }

    #go 1 dir up to bacteria (cdup = current dir up)
    $ftp->cdup();
}

