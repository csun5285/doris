#!/usr/bin/bash
curdir="$(cd "$(dirname $(readlink -f ${BASH_SOURCE[0]}))" &>/dev/null && pwd)"
selectdb_home="$(
  cd "${curdir}/.."
  pwd
)"

cd ${selectdb_home}

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
  echo "$0 must be invoked at the directory which contains bin, conf and lib"
  exit -1
fi

daemonized=0
for arg do
  shift
  [ "$arg" = "--daemonized" ] && daemonized=1 && continue
  [ "$arg" = "-daemonized" ] && daemonized=1 && continue
  [ "$arg" = "--daemon" ] && daemonized=1 && continue
  set -- "$@" "$arg"
done
# echo "$@" "daemonized=${daemonized}"}

process=selectdb_cloud

if [ -f ${selectdb_home}/bin/${process}.pid ]; then
  pid=$(cat ${selectdb_home}/bin/${process}.pid)
  if [ "${pid}" != "" ]; then
    ps axu | grep "$pid" 2>&1 | grep selectdb_cloud > /dev/null 2>&1
    if [ $? -eq 0 ]; then
      echo "pid file existed, ${process} have already started, pid=${pid}"
      exit -1;
    fi
  fi
  echo "pid file existed but process not alive, remove it, pid=${pid}"
  rm -f ${selectdb_home}/bin/${process}.pid
fi

lib_path=${selectdb_home}/lib
bin=${selectdb_home}/lib/selectdb_cloud
ldd ${bin} | grep -Ei 'libfdb_c.*not found' &> /dev/null
if [ $? -eq 0 ]; then
  if ! command -v patchelf &> /dev/null; then
    echo "patchelf is needed to launch meta_service"
    exit -1
  fi
  patchelf --set-rpath ${lib_path} ${bin}
  ldd ${bin}
fi

export JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:30000,dirty_decay_ms:30000,oversize_threshold:0,lg_tcache_max:16,prof:true,prof_prefix:jeprof.out"

mkdir -p ${selectdb_home}/log
echo "starts ${process} with args: $@"
if [ ${daemonized} -eq 1 ]; then
  date >> ${selectdb_home}/log/${process}.out
  nohup ${bin} "$@" >> ${selectdb_home}/log/${process}.out 2>&1 &
  # wait for log flush
  sleep 1.5
  tail -n10 ${selectdb_home}/log/${process}.out | grep 'working directory' -B1 -A10
  echo "please check process log for more details"
  echo ""
else
  ${bin} "$@"
fi

# vim: et ts=2 sw=2:
