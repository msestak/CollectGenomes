# NAME

CollectGenomes - Downloads genomes from Ensembl FTP (and NCBI nr db) and builds BLAST database (this is modulino - call it directly).

# SYNOPSIS

    Part I -> download genomes from Ensembl:

    perl ./lib/CollectGenomes.pm --mode=create_db -i . -ho localhost -d nr -p msandbox -u msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./lib/CollectGenomes.pm --mode=ensembl_ftp --out=./ensembl_ftp/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./lib/CollectGenomes.pm --mode=ensembl_vertebrates --out=./ensembl_vertebrates/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    Part II -> download genomes from NCBI:

    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /blast/db/FASTA/ -rf nr.gz

    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf gi_taxid_prot.dmp.gz

    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o ./nr -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf taxdump.tar.gz

    Part III -> load nr into database

    perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if ./nr/nr_10k.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB

    perl ./lib/CollectGenomes.pm --mode=gi_taxid -if ./nr/gi_taxid_prot.dmp.gz -o ./nr/ -ho localhost -u msandbox -p msandbox -d nr --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=InnoDB




    perl ./bin/CollectGenomes.pm --mode=ti_gi_fasta  -o . -d nr -ho localhost -u msandbox -p msandbox --port=5624 --socket=/tmp/mysql_sandbox5624.sock --engine=Deep

    perl ./bin/CollectGenomes.pm --mode=import_names -i ./t_eukarya/names_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./bin/CollectGenomes.pm --mode=import_nodes -i ./t_eukarya/nodes_martin7 -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    or
    perl blastdb_analysis.pl -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo -no nodes_martin7 -t 9606 -org hs -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    perl blastdb_analysis.pl -mode=call_phylo -no nodes_martin7 -t 2759 -org eu --proc=proc_create_phylo16278 -h localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./bin/CollectGenomes.pm --mode=get_existing_ti --in=./t_eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./bin/CollectGenomes.pm --mode=get_missing_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

    perl ./bin/CollectGenomes.pm --mode=delete_extra_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./bin/CollectGenomes.pm --mode=delete_full_genomes --in=. -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    perl ./bin/CollectGenomes.pm --mode=print_nr_genomes --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock

    perl ./bin/CollectGenomes.pm --mode=copy_existing_genomes --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya_old/  --out=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


    perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=./data_in/t_eukarya/ --out=./data_out/ -ho localhost -d nr -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    perl ./bin/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_29_07_15/data/archaea/ --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock


    perl ./bin/CollectGenomes.pm --mode=run_cdhit --in=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_29_07_15/data/cdhit/ -ho localhost -d nr -u msandbox -p msandbox -po 5622 -s /tmp/mysql_sandbox5622.sock -v

# DESCRIPTION

CollectGenomes is modulino that downloads genomes (actually proteomes) from Ensembl FTP servers. It names them by tax\_id.
It can also download NCBI nr database and extract genomes from it (requires MySQL).
It runs clustering with cd-hit and builds a BLAST database per species analyzed.

To use different functionality use specific modes.
Possible modes:

    create_db                     => \&create_database,
    ftp                           => \&ftp_robust,
    extract_nr                    => \&extract_nr,
    load_nr                       => \&load_nr,
    extract_and_load_nr           => \&extract_and_load_nr,
    gi_taxid                      => \&extract_and_load_gi_taxid,
    ti_gi_fasta                   => \&ti_gi_fasta,
    get_existing_ti               => \&get_existing_ti,
    import_names                  => \&import_names,
    import_nodes                  => \&import_nodes,
    get_missing_genomes           => \&get_missing_genomes,
    delete_extra_genomes          => \&delete_extra_genomes,
    delete_full_genomes           => \&delete_full_genomes,
    print_nr_genomes              => \&print_nr_genomes,
    copy_existing_genomes         => \&copy_existing_genomes,
    ensembl_vertebrates           => \&ensembl_vertebrates,
    ensembl_ftp                   => \&ensembl_ftp,
    prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
    run_cdhit                     => \&run_cdhit,

For help write:

    perl CollectGenomes.pm -h
    perl CollectGenomes.pm -m

# LICENSE

Copyright (C) MOCNII Martin Sebastijan Šestak

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

mocnii <msestak@irb.hr>
