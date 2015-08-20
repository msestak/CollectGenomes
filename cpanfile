requires 'perl', '5.010';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::Output';
};

requires 'strict';
requires 'warnings';
requires 'autodie';
requires 'lib';
requires 'Exporter';
requires 'Carp';
requires 'Data::Dumper';
requires 'Path::Tiny';
requires 'DBI';
requires 'DBD::mysql';
requires 'Getopt::Long';
requires 'Pod::Usage';
requires 'Capture::Tiny';
requires 'DateTime::Tiny';
requires 'Log::Log4perl';
requires 'Net::FTP';
requires 'Net::FTP::Robust';
requires 'Net::FTP::AutoReconnect';
requires 'PerlIO::gzip';
requires 'Archive::Extract';
requires 'POSIX';
requires 'IO::Prompter';
requires 'File::Find::Rule';
requires 'FindBin';
requires 'HTML::TreeBuilder';
requires 'LWP::Simple';
requires 'DateTime::Tiny';


author_requires 'Regexp::Debugger';

