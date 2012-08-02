#!/bin/bash
# vim: ts=8 sw=2 smarttab

our_ceph_conf="ceph.conf.test-trim"
test_create_osds_total=50
output_dir=~/test-trim
store_path=dev/mon.a/store.db

test_run=`cat ~/test-trim/.last`
test_run=$(($test_run+1))

test_run_dir=$output_dir/run.$test_run
mkdir $test_run_dir

echo "* remove out/ dev/ keyring"
rm -fr out/ dev/ keyring
echo "* create osd dirs"
mkdir -p dev/osd0 dev/osd1
echo "* copy $our_ceph_conf to ceph.conf"
cp $our_ceph_conf ceph.conf

echo "* run vstart.sh"
CEPH_NUM_OSD=2 ./vstart.sh -n -x -l -k

echo "* run test sequence:"
echo "  - create $test_create_osds_total osds"
for i in `seq 1 $test_create_osds_total`; do
  ./ceph osd create `uuidgen`;
done

last_osd=$(($test_create_osds_total+2-1))
echo "  - mark osd.2 .. $last_osd lost"
for i in `seq 2 $last_osd`; do
  ./ceph osd lost $i --yes-i-really-mean-it ;
done

echo "  - remove osd.2 .. $last_osd"
for i in `seq 2 $last_osd`; do
  ./ceph osd rm $i --yes-i-really-mean-it ;
done

echo "  - remove osd.1"
./ceph osd rm 1 --yes-i-really-mean-it ;

echo "* sleeping for a bit"
sleep 30

echo "* killing the cluster"
./init-ceph stop

echo "* comparing logs"
#cat out/mon.a.log | grep "propose_queued [[:digit:]]" | cut -f6- -d' ' > $test_run_dir/proposals
cat out/mon.a.log | grep "trim_to[[:digit:]]" | cut -f5- -d' ' > $test_run_dir/trims

#for i in proposals trims; do
for i in trims; do
  echo "  - $i:"
  for j in osdmap pgmap log; do
    echo -n "    $j: "
    cat $test_run_dir/$i | grep $j > $test_run_dir/$i.$j ;
    diff $output_dir/test-trim.$i.$j $test_run_dir/$i.$j >& /dev/null
    ret=$?
    if [[ $ret -eq 0 ]]; then
      echo "OK"
    else
      diff_line=`diff $output_dir/test-trim.$i.$j $test_run_dir/$i.$j | head -n 1`
      echo $diff_line | grep a >& /dev/null
      if [[ $? -eq 0 ]]; then
        goes_into=`echo $diff_line | cut -f1 -da`
        comes_from=`echo $diff_line | cut -f2 -da | cut -f1 -d,`
      else
        echo $diff_line | grep d >& /dev/null
        if [[ $? -eq 0 ]]; then
          goes_into=`echo $diff_line | cut -f2 -dd`
          comes_from=`echo $diff_line | cut -f1 -dd | cut -f1 -d,`
        else
          echo "mismatch"
          continue ;
        fi
      fi
      expected_at=$(($comes_from-1))
      if [[ $expected_at -eq $goes_into ]]; then
        echo "there were additional events, but it's okay"
      else
        echo "mismatch"
      fi
    fi
  done
done

echo "* checking store"
for i in osdmap pgmap log; do
  echo "  - check $i versions: "
  last_trim=`cat $test_run_dir/trims.$i | sed -e 's/^.*trim_to//' | sort -g | tail -n 1`
  first_existing_version=$(($last_trim+1))
  error=0
  for v in `seq 1 $last_trim`; do
    version_key=$v
    full_key="full_$v"
    ./mon-store-tool $store_path exists $i $version_key >& /dev/null
    if [[ $? -eq 0 ]]; then
      echo "    unexpected: ${i}_$version_key exists"
      error=1
    fi
    ./mon-store-tool $store_path exists $i $full_key >& /dev/null
    if [[ $? -eq 0 ]]; then
      echo "    unexpected: ${i}_$full_key exists"
      error=1
    fi
  done
  ./mon-store-tool $store_path exists $i $first_existing_version >& /dev/null
  if [[ $? -eq 1 ]]; then
    echo "    unexpected: ${i}_$first_existing_version does not exist"
    error=1
  fi
  if [[ $error -eq 0 ]]; then
    echo "    OKAY"
  else
    echo "    ERROR"
  fi
done

echo $test_run > $output_dir/.last
