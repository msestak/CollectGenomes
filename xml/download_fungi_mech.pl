#!/usr/bin/env perl
use strict;
use warnings;
use Path::Tiny;
use Data::Dumper;
use WWW::Mechanize;
use HTTP::Cookies::Netscape;

#get xml files of large groups
my @jgi_zomes = qw(
http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=Metazome
http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=PhytozomeV10
http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi
http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi
);

my $OUT = $ARGV[0] or path('.');

my $cookie_jar = HTTP::Cookies::Netscape->new(
   file => "cookie_jgi",
);

#my $mech = WWW::Mechanize->new(
#    cookie_jar => {
#        file     => "jgi_cookie",
#        autosave => 1
#      }
#
#);

my $mech = WWW::Mechanize->new( cookie_jar => $cookie_jar, autocheck => 1 );

http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi

foreach my $URL (@jgi_zomes) {
	(my $xml_name = $URL) =~ s{\A(?:.+?)organism=(.+)\z}{$1};
	my $xml_path = path($OUT, $xml_name . '.xml')->canonpath;
	#getstore($URL, $xml_path);

	my $response = $mech->get($URL);
	$mech->save_content( $xml_path);

	print "Cookie:\n" . $cookie_jar->as_string;


}

