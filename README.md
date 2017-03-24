# NAME

CollectGenomes - Downloads genomes from Ensembl FTP (and NCBI nr db) and builds BLAST database (this is modulino - call it directly).

# SYNOPSIS

    ### Part 0 -> prepare the stage:
    # Step1: create a MySQL database named by date
    perl ./lib/CollectGenomes.pm --mode=create_db -ho localhost -d nr_2015_9_2 -p msandbox -u msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    # Step2: create a collection of directories inside a db directory (need to create manually - by date) to store all db files and preparation
    #also copies update_phylogeny file which is manually curated in doc directory
    perl ./lib/CollectGenomes.pm --mode=make_db_dirs -o /home/msestak/dropbox/Databases/db_02_09_2015/ -if /home/msestak/dropbox/Databases/db_29_07_15/doc/update_phylogeny_martin7.tsv

    ### Part I -> download genomes from Ensembl:
    # Step 1: download protists, fungi, metazoa and bacteria (21085)
    perl ./lib/CollectGenomes.pm --mode=ensembl_ftp --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_ftp/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    perl /msestak/gitdir/CollectGenomes/t/ftp_get_bacteria.pl

    # Step 2: download proteomes
    CollectGenomes.pm --mode=download_from_stats --out=/msestak/workdir/nr_22_03_2017/data/ensembl_ftp/ --infile=/msestak/workdir/nr_22_03_2017/statistics_ensembl_all.txt --tables info=species_ensembl_divisions25579 -ho localhost -d nr_22_03_2017 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v -v

    # Step 3: download vertebrates
    #need to scrape HTML to get to taxids in order to download vertebrates from Ensembl (+78 = total 21163) downloaded 67 vertebrates + 2 (S.cerevisiae and C. elegans) + 27 PRE (but duplicates (real 11))
    perl ./lib/CollectGenomes.pm --mode=ensembl_vertebrates --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_vertebrates/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #copy ensembl proteomes to ensembl_all (7 min)
    time cp ./ensembl_ftp/* ./ensembl_all/
    cp -i ./ensembl_vertebrates/* ./ensembl_all/
    cp: overwrite `./ensembl_all/4932'? y   (S. cerevisiae)
    cp: overwrite `./ensembl_all/6239'? y   (C. elegans)

    
    ### Part II -> download genomes from NCBI:
    # Step1: download NCBI nr protein fasta file, gi_taxid_prot and taxdump
    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /blast/db/FASTA/ -rf nr.gz
    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf gi_taxid_prot.dmp.gz
    perl ./lib/CollectGenomes.pm --mode=nr_ftp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -rh ftp.ncbi.nih.gov -rd /pub/taxonomy/ -rf taxdump.tar.gz
    #taxdmp is needed for names and nodes files (phylogeny information)
    [msestak@tiktaalik nr_raw]$ tar -xzvf taxdump.tar.gz
    [msestak@tiktaalik nr_raw]$ rm citations.dmp delnodes.dmp gc.prt merged.dmp gencode.dmp


    ### Part IIa -> download genomes from JGI:
    perl ./lib/CollectGenomes.pm --mode=jgi_download --names=names_raw_2015_9_3_new -tbl gold=gold_ver5 -o /home/msestak/dropbox/Databases/db_02_09_2015/data/xml/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock

    ### Part III -> load nr into database:
    # Step1: load gi_taxid_prot to connect gi from nr and ti from gi_taxid_prot
    perl ./lib/CollectGenomes.pm --mode=gi_taxid -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
    #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz has 223469419 lines!
    #File /home/msestak/gitdir/CollectGenomes/t/nr/gi_taxid_prot_TokuDB written with 223469419 lines!
    #Report: import inserted 223469419 rows in 3331 sec (67087 rows/sec)
    # Step2: load full nr NCBI database
    perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
    #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz has 70614921 lines!
    #File /home/msestak/gitdir/CollectGenomes/t/nr/nr_2015_9_3_TokuDB written with 211434339 lines!
    #Report: import inserted 211434339 rows! in 28969 sec (7298 rows/sec)
    
    ### Part IV -> set phylogeny for focal species:
    # Step1: load raw names and nodes and prune nodes of Viruses and other unwanted sequences
    perl ./lib/CollectGenomes.pm --mode=import_raw_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
    #Action: inserted 1987756 rows to names:names_dmp in 81 sec (24540 rows/sec)
    #PRUNING partI: excluded Phages, Viruses, Sythetic and Environmental samples while loading nodes_dmp
    perl ./lib/CollectGenomes.pm --mode=import_raw_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
    #Action: inserted 1124194 rows to nodes:nodes_dmp in 45 sec (24982 rows/s)
    
    # Step2: import tis of Ensembl genomes, count them and get a list of files for MakeTree
    perl ./lib/CollectGenomes.pm --mode=get_ensembl_genomes --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/ensembl_all/ --tables names=names_raw_2015_9_3_new -o /home/msestak/dropbox/Databases/db_02_09_2015/doc/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -en=TokuDB
    #Action: update to ensembl_genomes updated 21163 rows!
    #Report: table ensembl_genomes has 21163 rows

    # Step3: run MakeTree to get modified phylogeny
    [msestak@tiktaalik db_02_09_2015]$ MakeTree -m ./data/nr_raw/names_raw_2015_9_3 -n ./data/nr_raw/nodes_raw_2015_9_3 -i ./doc/update_phylogeny_martin7.tsv -d 3 -s ./doc/ensembl -t 6072 | TreeIlustrator.pl
    Eumetazoa[6072]
    ├─Placozoa[10226]
    │ └─Trichoplax[10227]
    │   └─Trichoplax_adhaerens[10228]
    └─Cnidaria/Bilateria[1708696]
      ├─Cnidaria[6073]
      │ ├─Medusozoa[1708697]
      │ └─Anthozoa[6101]
      └─Bilateria[33213]
        ├─Deuterostomia[33511]
        └─Protostomia[33317]

    ---------------------------------------------
    Modified names and nodes file can be found in :
    ---------------------------------------------

    Nodes: ./data/nr_raw/nodes_raw_2015_9_3.new
    Names: ./data/nr_raw/names_raw_2015_9_3.new
    
    # Step4: import modified names and nodes to database
    perl ./lib/CollectGenomes.pm --mode=import_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
    perl ./lib/CollectGenomes.pm --mode=import_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB

    # Step5: create phylo tables for Other(28384 for pruning) and Species of interest (here 7955 Danio rerio)
    perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
    perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 28384 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v --engine=TokuDB
    
    # Step6: PRUNING partII: delete rest of Other sequences (28384 most deleted in loading raw nodes - Synthetic)
    perl ./lib/CollectGenomes.pm -mode=del_virus_from_nr -tbl nr=gi_taxid_prot_TokuDB -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v

    # Step7: PRUNING partIII: delete all taxids that are present in gi_ti_prot_dmp table but not in updated nodes table
    #also delete all taxids which are not leaf nodes (species)
    perl ./lib/CollectGenomes.pm -mode=del_missing_ti -tbl nr=gi_taxid_prot_TokuDB -tbl nodes=nodes_raw_2015_9_3_new -tbl names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -v
    #Report: deleted total of 2630957 rows in mode: genera

    ### Part V -> get genomes from nr base:
    # Step1: long running - JOIN of nr base and gi_taxid_prot table
    perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5625 --socket=/tmp/mysql_sandbox5625.sock --engine=TokuDB
    #Report: import inserted 204044303 rows in 25266 sec (8075 rows/sec)

    # Step2: COUNT all genomes by taxid
    perl ./lib/CollectGenomes.pm --mode=nr_genome_counts --tables nr=nr_ti_gi_fasta_TokuDB --tables names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
    #Action: import to nr_ti_gi_fasta_TokuDB_cnt inserted 455063 rows in 900 sec 
    #Action: update to nr_ti_gi_fasta_TokuDB_cnt updated 455063 rows!
    
    ### Part VI -> combine nr genomes with Ensembl genomes and print them out:
    # Step1:delete genomes from nr_cnt table that are present in ensembl_genomes (downloaded from Ensembl)
    #it also deletes genomes smaller than 2000 sequences
    #it also deletes all genomes having 'group' in name
    #prints report at end
    perl ./lib/CollectGenomes.pm --mode=get_missing_genomes --tables nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock --engine=TokuDB
    #Action: table nr_ti_gi_fasta_TokuDB_cnt deleted 21139 rows!
    #Action: table nr_ti_gi_fasta_TokuDB_cnt deleted 427679 rows!
    #Action: deleted 20 groups from table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 6225 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 4854 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 3533 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 2589 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1981 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1562 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1296 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1087 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 959 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 618 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 468 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 373 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 276 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt

    # Step2: delete genomes with species and strain genomes overlaping (nr only)
    perl ./lib/CollectGenomes.pm --mode=del_nr_genomes -tbl nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #Report: found 6096 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 4750 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 3446 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 2510 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1907 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1494 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1232 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 1027 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 900 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 565 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 417 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 327 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    #Report: found 170 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_TokuDB_cnt
    
    # Step3: imports nr and existing genomes
    perl ./lib/CollectGenomes.pm --mode=del_total_genomes -tbl nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock -en=TokuDB
    #Action: import inserted 6096 rows!
    #Action: import inserted 21163 rows!
    #Action: deleted 2 hybrid species from ti_full_list
    #Report: found 26265 genomes in table:ti_full_list
    
    # Step4: extract nr genomes after filtering
    perl ./lib/CollectGenomes.pm --mode=print_nr_genomes -tbl ti_full_list=ti_full_list -tbl nr_ti_fasta=nr_ti_gi_fasta_TokuDB -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_genomes/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #printed 5162 genomes

    # Step5: remove genomes from jgi that are found in nr or ensembl (to jgi_clean directory)
    perl ./lib/CollectGenomes.pm --mode=copy_jgi_genomes -tbl ti_full_list=ti_full_list -tbl names=names_raw_2015_9_3_new --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/jgi/ --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/jgi_clean/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    # Action: update to ti_full_list updated 221 rows!
    # Report: found 219 JGI genomes in table:ti_full_list

    # Step6: copy genomes (external) from previous database not in this one
    perl ./lib/CollectGenomes.pm --mode=copy_external_genomes -tbl ti_full_list=ti_full_list -tbl names=names_raw_2015_9_3_new --in=/home/msestak/dropbox/Databases/db_29_07_15/data/eukarya --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/external/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    # Action: update to ti_full_list updated 168 rows!
    # Report: found 167 external genomes in table:ti_full_list

    # Step7: delete duplicates from final database
    perl ./lib/CollectGenomes.pm --mode=del_species_with_strain -tbl ti_full_list=ti_full_list -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #Action: deleted 1 hybrid species from ti_full_list
    #10 genomes deleted
    #Report: found 26589 genomes in table:ti_full_list

    # Step8: merge jgi, nr, external, ensembl genomes to all:
    # it deletes genomes with taxid < 100 because of Centos6 kernel Boost issue in MakePhyloDb
    perl ./lib/CollectGenomes.pm --mode=merge_existing_genomes -o /home/msestak/dropbox/Databases/db_02_09_2015/data/all/ -tbl ti_full_list=ti_full_list -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #Copied 26586 genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all (40.2 GB)
    #Copied 214 JGI genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
    #Copied 5147 NCBI genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
    #Copied 145 external genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all
    #Copied 21067 Ensembl genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/all

    ### Part VII -> prepare and run cd-hit
    # Step1: run MakePhyloDb to get pgi||ti|pi|| identifiers (7h) and .ff extension
    [msestak@tiktaalik data]$ cp ./all_raw/ ./all_sync/
    [msestak@tiktaalik data]$ MakePhyloDb -d ./all_sync/

    # Step2: remove .ff from genomes that are not leaf nodes
    # and put nodes on 0 that are behind genome node
    DbSync.pl -d ./all_sync/ -n ./nr_raw/nodes.dmp.fmt.new
    mv ./all_sync/*.ff ./all_ff/
    #to update statistics
    [msestak@tiktaalik data]$ mv ./all_sync/*.ff ./all_ff/
    [msestak@tiktaalik data]$ ls ./all_sync/ | wc -l
    #1331
    [msestak@tiktaalik data]$ ls ./all_ff/ | wc -l
    #25244
    #copy info files to update them
    [msestak@tiktaalik data]$ cp ./all_sync/info.* ./all_ff/
    #update info files for Phylostrat
    [msestak@tiktaalik data]$ MakePhyloDb -d ./all_ff/
    [msestak@tiktaalik data]$ cat ./all_ff/info.paf 
    #2015-9-24.13:45:35 :Database Created On:
    #25244 :Number Of Genomes:
    #38538642861 :Database Size:
    #37564616819 :Effective Database Size:
    [msestak@tiktaalik data]$ cat ./all_sync/info.paf 
    #2015-9-22.18:47:19 :Database Created On:
    #26573 :Number Of Genomes:
    #41516744334 :Database Size:
    #40475950304 :Effective Database Size:

    # Step3: analyze database
    [msestak@tiktaalik data]$ AnalysePhyloDb -d ./all_ff/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_25244_genomes_danio
    [msestak@tiktaalik data]$ grep "<ps>" analyze_25244_genomes_danio > analyze_25244_genomes_danio.ps
    [msestak@tiktaalik data]$ cat analyze_25244_genomes_danio.ps 
    #<ps>  1       19665   131567
    #<ps>  2       266     2759
    #<ps>  3       13      1708629
    #<ps>  4       1       1708631
    #<ps>  5       787     33154
    #<ps>  6       2       1708671
    #<ps>  7       1       1708672
    #<ps>  8       2       1708673
    #<ps>  9       7       33208
    #<ps>  10      1       6072
    #<ps>  11      8       1708696
    #<ps>  12      133     33213
    #<ps>  13      2       33511
    #<ps>  14      1       7711
    #<ps>  15      3       1708690
    #<ps>  16      1       7742
    #<ps>  17      1       7776
    #<ps>  18      0       117570
    #<ps>  19      167     117571
    #<ps>  20      0       7898
    #<ps>  21      0       186623
    #<ps>  22      1       41665
    #<ps>  23      1       32443
    #<ps>  24      1       1489341
    #<ps>  25      22      186625
    #<ps>  26      1       186634
    #<ps>  27      0       32519
    #<ps>  28      2       186626
    #<ps>  29      0       186627
    #<ps>  30      0       7952
    #<ps>  31      0       30727
    #<ps>  32      2       7953
    #<ps>  33      0       7954
    #<ps>  34      1       7955
    [msestak@tiktaalik data]$ grep -P "^\d+\t" analyze_25244_genomes_danio > analyze_25244_genomes_danio.genomes

    # Step 3b: remove genomes found in all_ff directory but not found in AnalysePhyloDb file (not found in nodes.dmp.fmt.new.sync because at 0) -> deleted before
    perl ./lib/CollectGenomes.pm --mode=del_after_analyze -i /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff/ -if /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_all_ff -o /home/msestak/dropbox/Databases/db_02_09_2015/data/all_sync/
    #Report: found 25244 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff
    #Report: found 25224 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_all_ff
    #Report: removed 20 genomes out of /home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff to /home/msestak/dropbox/Databases/db_02_09_2015/data/all_sync

    # Step4: partition genomes per phylostrata for cdhit
    perl ./lib/CollectGenomes.pm --mode=prepare_cdhit_per_phylostrata --in=/home/msestak/dropbox/Databases/db_02_09_2015/data/all_ff/ --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ -tbl phylo=phylo_7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #Report: 26 phylostrata:{ps1 ps2 ps3 ps4 ps5 ps6 ps7 ps8 ps9 ps10 ps11 ps12 ps13 ps14 ps15 ps16 ps17 ps19 ps22 ps23 ps24 ps25 ps26 ps28 ps32 ps34}
    #Action: dir /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2 removed and cleaned
    #Action: concatenated 23757 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps1.fa
    #Action: concatenated 277 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps2.fa
    #Action: concatenated 13 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps3.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4.fa
    #Report: ps4 has 1 genomes and is excluded for cdhit
    #Action: File /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4.fa renamed to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps4
    #Action: concatenated 806 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps5.fa
    #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps6.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps7.fa
    #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps8.fa
    #Action: concatenated 7 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps9.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps10.fa
    #Action: concatenated 8 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps11.fa
    #Action: concatenated 134 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps12.fa
    #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps13.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps14.fa
    #Action: concatenated 3 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps15.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps16.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps17.fa
    #Action: concatenated 167 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps19.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps22.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps23.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps24.fa
    #Action: concatenated 22 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps25.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps26.fa
    #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps28.fa
    #Action: concatenated 2 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps32.fa
    #Action: concatenated 1 files to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/ps34.fa
    
    # Step5: run cdhit based on cd_hit_cmds file
    [msestak@tiktaalik CollectGenomes]$ perl ./lib/CollectGenomes.pm --mode=run_cdhit --if=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/cd_hit_cmds_ps1 --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/
    #run ps1 separately
    [msestak@cambrian-0-0 CollectGenomes]$ perl ./lib/CollectGenomes.pm --mode=run_cdhit --if=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/cd_hit_cmds --out=/home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit2/

    # Step5: combine all cdhit files into one db and replace J to * for BLAST
    perl ./lib/CollectGenomes.pm --mode=cdhit_merge -i /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/ -of /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/blast_db -o /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
    #Report: printed 43923562 fasta records to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/blast_db (18.1 GB)
    #Report: printed 22290 genomes to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
    
    # Step6: add some additional genomes to database
    perl ./lib/CollectGenomes.pm --mode=manual_add_fasta -if ./cdhit/V2.0.CommonC.pfasta -o ./cdhit/ -t 7962
    #Report: transformed /msestak/gitdir/CollectGenomes/cdhit/V2.0.CommonC.pfasta to /msestak/gitdir/CollectGenomes/cdhit/7962 (46609 rows) with BLAST_format = true

    # Step7: rum MakePhyloDb and AnalysePhyloDb again to get accurate info after cdhit
    [msestak@tiktaalik data]$ MakePhyloDb -d ./cdhit_large/extracted/
    [msestak@tiktaalik data]$ AnalysePhyloDb -d ./cdhit_large/extracted/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_cdhit_large

    [msestak@tiktaalik data]$ MakePhyloDb -d ./cdhit_large/extracted/
    [msestak@tiktaalik data]$ AnalysePhyloDb -d ./cdhit_large/extracted/ -t 7955 -n ./nr_raw/nodes.dmp.fmt.new.sync > analyze_cdhit_large
    [msestak@tiktaalik data]$ grep -P "^\d+\t" analyze_cdhit_large > analyze_cdhit_large.genomes
    [msestak@tiktaalik data]$ wc -l analyze_cdhit_large.genomes 
    #22290 analyze_cdhit_large.genomes
    [msestak@tiktaalik data]$ mkdir ./cdhit_large/surplus
    perl ./lib/CollectGenomes.pm --mode=del_after_analyze -i /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted/ -if /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_cdhit_large -o /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/surplus/
    #Report: found 22290 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted
    #Report: found 22290 genomes in /home/msestak/dropbox/Databases/db_02_09_2015/data/analyze_cdhit_large
    #Report: removed 0 genomes out of /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/extracted to /home/msestak/dropbox/Databases/db_02_09_2015/data/cdhit_large/surplus



    ### Part VIII -> prepare for BLAST
    # Step1: get longest splicing var
    [msestak@tiktaalik in]$ SplicVar.pl -f Danio_rerio.GRCz10.pep.all.fa -l L > danio_splicvar 
    [msestak@tiktaalik in]$ grep -c ">" Danio_rerio.GRCz10.pep.all.fa 
    #44487
    [msestak@tiktaalik in]$ grep -c ">" danio_splicvar
    #25638
    perl ./lib/FastaSplit.pm -if /msestak/workdir/danio_dev_stages_phylo/in/dr_splicvar -name dr -o /msestak/workdir/danio_dev_stages_phylo/in/in_chunks_dr -n 50 -s 7000 -a
    #Num of seq: 25638
    #Num of chunks: 50
    #Num of seq in chunk: 512
    #Num of seq left without chunk: 38
    #Larger than 7000 {7 seq}: 27765 22190 9786 8864 8710 8697 7035

    ### Part IX -> backup a database
    /home/msestak/gitdir/CollectGenomes/lib/CollectGenomes.pm --mode=mysqldump -d blastdb -o . -u msandbox --password=msandbox --port=5622 --socket=/tmp/mysql_sandbox5622.sock -v -v

# DESCRIPTION

CollectGenomes is modulino that downloads genomes (actually proteomes) from Ensembl FTP servers. It names them by tax\_id.
It can also download NCBI nr database and extract genomes from it (requires MySQL).
It runs clustering with cd-hit and builds a BLAST database per species analyzed.

To use different functionality use specific modes.
Possible modes:

    create_db                     => \&create_db,
    ftp                           => \&ftp_robust,
    extract_nr                    => \&extract_nr,
    extract_and_load_nr           => \&extract_and_load_nr,
    gi_taxid                      => \&extract_and_load_gi_taxid,
    ti_gi_fasta                   => \&ti_gi_fasta,
    get_ensembl_genomes               => \&get_ensembl_genomes,
    import_names                  => \&import_names,
    import_nodes                  => \&import_nodes,
    get_missing_genomes           => \&get_missing_genomes,
    del_nr_genomes          => \&del_nr_genomes,
    del_total_genomes           => \&del_total_genomes,
    print_nr_genomes              => \&print_nr_genomes,
    merge_existing_genomes         => \&merge_existing_genomes,
    ensembl_vertebrates           => \&ensembl_vertebrates,
    ensembl_ftp                   => \&ensembl_ftp,
    prepare_cdhit_per_phylostrata => \&prepare_cdhit_per_phylostrata,
    run_cdhit                     => \&run_cdhit,

For help write:

    perl CollectGenomes.pm -h
    perl CollectGenomes.pm -m

# EXAMPLE 02.09.2015 on tiktaalik

    ALTERNATIVE with Deep:
    perl ./lib/CollectGenomes.pm --mode=create_db -ho localhost -d nr_2015_9_2 -p msandbox -u msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock
    perl ./lib/CollectGenomes.pm --mode=gi_taxid -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
    #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/gi_taxid_prot.dmp.gz has 223469419 lines!
    #File /home/msestak/gitdir/CollectGenomes/t/nr/gi_taxid_prot_Deep written with 223469419 lines!
    #import inserted 223469419 rows! in 3381 sec (66095 rows/sec)
    
    perl ./lib/CollectGenomes.pm --mode=extract_and_load_nr -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz -o ./t/nr/ -ho localhost -u msandbox -p msandbox -d nr_2015_9_2 --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
    #File /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nr.gz has 70614921 lines!
    #File /home/msestak/gitdir/CollectGenomes/t/nr/nr_2015_9_3_Deep written with 211434339 lines!
    #import inserted 211434339 rows! in 20447 sec (10340 rows/sec)
    #copy missing tables to other MySQL server
    mysqldump nr_2015_9_2 species_ensembl_divisions -u msandbox -p'msandbox' --single-transaction --port=5625 --socket=/tmp/mysql_sandbox5625.sock | mysql -D nr_2015_9_2 -u msandbox -p'msandbox' --port=5626 --socket=/tmp/mysql_sandbox5626.sock
    perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep

    ### Part IV -> set phylogeny for focal species:

    perl ./lib/CollectGenomes.pm --mode=import_raw_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
    #Action: inserted 1987756 rows to names:names_dmp in 69 sec (28808 rows/sec)
    perl ./lib/CollectGenomes.pm --mode=import_raw_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes.dmp -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
    #Action: inserted 1124194 rows to nodes:nodes_dmp in 31 sec (36264 rows/s)
    perl ./lib/CollectGenomes.pm --mode=import_names -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/names_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
    perl ./lib/CollectGenomes.pm --mode=import_nodes -if /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_raw/nodes_raw_2015_9_3.new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep

    perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 7955 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v --engine=Deep
    perl ./lib/CollectGenomes.pm -mode=fn_tree,fn_retrieve,prompt_ph,proc_phylo,call_phylo -no nodes_raw_2015_9_3_new -t 28384 -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v --engine=Deep


    #PRUNING partII: delete rest of Other sequences (most deleted in loading raw nodes - Synthetic)
    perl ./lib/CollectGenomes.pm -mode=del_virus_from_nr -tbl nr=gi_taxid_prot_Deep -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v
    #PRUNING partIII: delete all taxids that are present in gi_ti_prot_dmp table but not in updated nodes table
    #also delete all taxids which are not leaf nodes (species)
    perl ./lib/CollectGenomes.pm -mode=del_missing_ti -tbl nr=gi_taxid_prot_Deep -tbl nodes=nodes_raw_2015_9_3_new -tbl names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -v
    #Report: deleted total of 2630957 rows in mode: genera
    
    ### Part V -> get genomes from nr base:
    perl ./lib/CollectGenomes.pm --mode=ti_gi_fasta -d nr_2015_9_2 -ho localhost -u msandbox -p msandbox --port=5626 --socket=/tmp/mysql_sandbox5626.sock --engine=Deep
    #Report: import inserted 204044303 rows in 5312 sec (38411 rows/sec)

    #perl ./lib/CollectGenomes.pm --mode=nr_genome_counts --tables nr=nr_ti_gi_fasta_Deep --tables names=names_raw_2015_9_3_new -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock --engine=Deep
    #Action: import to nr_ti_gi_fasta_Deep_cnt inserted 455063 rows in 200 sec
    #Action: update to nr_ti_gi_fasta_Deep_cnt updated 455063 rows!
    
    ### Part VI -> combine nr genomes with Ensembl genomes and print them out:
    #deletes genomes from nr_cnt table that are present in ensembl_genomes (downloaded from Ensembl)
    #it also deletes genoes smaller than 2000 sequences
    perl ./lib/CollectGenomes.pm --mode=get_missing_genomes --tables nr_cnt=nr_ti_gi_fasta_TokuDB_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5625 -s /tmp/mysql_sandbox5625.sock
    #Action: table nr_ti_gi_fasta_Deep_cnt deleted 21139 rows!
    #Action: table nr_ti_gi_fasta_Deep_cnt deleted 427679 rows!
    #Report: found 6245 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 4870 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 3543 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 2598 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1990 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1571 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1304 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1093 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 965 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 620 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 469 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 374 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 26 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    
    perl ./lib/CollectGenomes.pm --mode=del_nr_genomes -tbl nr_cnt=nr_ti_gi_fasta_Deep_cnt -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock
    #deletes genomes with species and strain genomes overlaping (only nr)
    #Report: found 5928 genomes larger than 2000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 4600 genomes larger than 3000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 3325 genomes larger than 4000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 2404 genomes larger than 5000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1805 genomes larger than 6000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1396 genomes larger than 7000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 1139 genomes larger than 8000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 939 genomes larger than 9000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 813 genomes larger than 10000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 501 genomes larger than 15000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 365 genomes larger than 20000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 285 genomes larger than 25000 proteins in table:nr_ti_gi_fasta_Deep_cnt
    #Report: found 3 genomes larger than 300000 proteins in table:nr_ti_gi_fasta_Deep_cnt

    perl ./lib/CollectGenomes.pm --mode=del_total_genomes -tbl nr_cnt=nr_ti_gi_fasta_Deep_cnt -tbl ensembl_genomes=ensembl_genomes -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock -en=Deep
    #imports nr and existing genomes
    #deletes hybrid genomes
    #Action: import inserted 5928 rows!
    #Action: import inserted 21163 rows!
    #Action: deleted 2 hybrid species from ti_full_list
    #Report: found 25063 genomes in table:ti_full_list

    #extract nr genomes after filtering
    perl ./lib/CollectGenomes.pm --mode=print_nr_genomes -tbl ti_full_list=ti_full_list -tbl nr_ti_fasta=nr_ti_gi_fasta_Deep -o /home/msestak/dropbox/Databases/db_02_09_2015/data/nr_genomes/ -ho localhost -d nr_2015_9_2 -u msandbox -p msandbox -po 5626 -s /tmp/mysql_sandbox5626.sock

# LICENSE

Copyright (C) Martin Sebastijan Šestak.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Martin Sebastijan Šestak <msestak@irb.hr>
