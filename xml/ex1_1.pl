#!/bin/perl -w

#########################################################################
#                                                                       #
#  This first example shows how to create a twig, parse a file into it  #
#  get the root of the document, its children, access a specific child  #
#  and get the text of an element                                       #
#                                                                       #
#########################################################################

use strict;
use 5.010;
use XML::Twig;
use Data::Dumper;
use LWP::Simple qw(getstore);
use LWP::UserAgent;

my $xml= $ARGV[0];
my $twig= new XML::Twig(pretty_print => 'indented');

$twig->parsefile( $xml );    # build the twig
my $root= $twig->root;           # get the root of the twig (stats)
#say 'root:', Dumper($root);
my @folders_upper = $root->children;    # get the player list
#say 'players:',Dumper(\@players);







#my $folder_upper   = $twig->first_elt( 'folder' );
#$folder_upper->print;
foreach my $folder_upper (@folders_upper) {
	my $species_name = $folder_upper->att( 'name' );
	say 1;
	say "{$species_name}";
	
	
	my @folders= $folder_upper->children;
	say "@folders";
	
	foreach my $folder (@folders) {
		my @files = $folder->children;
		say "@files";
		foreach my $file (@files) {
			#$file->print;
			#print "\n";
			#<file filename="Xtropicalis_14_gene_exons.gff3.gz" size="4 MB" url="/ext-api/downloads/get_tape_file?blocking=true&amp;url=/Metazome/download/_JAMO/53f398a00d878557fd3b7489/Xtropicalis_14_gene_exons.gff3.gz"/>
			my $filename = $file->att( 'filename' );
			say "filename:$filename";
			my $size = $file->att( 'size' );
			say "size:$size";
			my $url = $file->att( 'url' );
			say "url:$url";
			$url =~ s{/ext-api(?:.+?)url=(.+)}{$1};
			say $url;
			$url = 'http://genome.jgi.doe.gov' . $url;
			say $url;

			my $ua = LWP::UserAgent->new;
			$ua->credentials("http://genome.jgi.doe.gov", "JGI", 'msestak@irb.hr', 'jgi_for_lifem8');
			my $res = $ua->get($url);
			if ($res->is_success) {
			   print "ok\n";
			   getstore($url, $filename);
			}
			else {
			   print $res->status_line, "\n";
			}
		}
	}
}
