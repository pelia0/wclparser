"""Exercise download controls against the actual browser script with a small DOM."""
import shutil
import subprocess

from wcl_archiver.config import BASE_DIR


def test_download_controls_apply_validation_polling_and_separate_disk_status():
    script = r'''
const fs=require('fs'), vm=require('vm'), assert=require('assert');
class Element {
  constructor(){this.value='';this.textContent='';this.style={};this.dataset={};this.disabled=false;this.validity='';}
  setCustomValidity(value){this.validity=value;}
  reportValidity(){return !this.validity;}
  addEventListener(){}
}
const fields={};
const document={activeElement:null,getElementById(id){return fields[id] ||= new Element();}};
const calls=[];
let response={download_concurrency:10,browser_request_concurrency:20,pack_workers:4,
              staged_fights:7,staging_files:8,staging_bytes:2097152,packing_fights:3,
              downloading_fights:6,preparing_reports:2};
let ok=true;
const context={document,window:{addEventListener(){}},console,URLSearchParams,
 fetch:async(url,options)=>{calls.push({url,options});return {ok,json:async()=>response};}};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[1],'utf8'),context);
(async()=>{
  vm.runInContext('renderHarvesterState({download_concurrency:4,browser_request_concurrency:8,pack_workers:4})',context);
  assert.equal(fields['stat-downloading'].textContent,'Restart server for active count');
  const input=fields['harvest-download-threads'];
  assert.equal(input.value,'4');
  input.value='10';input.dataset.dirty='1';
  vm.runInContext('renderHarvesterState({download_concurrency:4,browser_request_concurrency:8,pack_workers:4})',context);
  assert.equal(input.value,'10','polling must not erase an unapplied edit');
  await vm.runInContext('applyDownloadConcurrency()',context);
  assert.equal(calls[0].url,'/api/harvester/settings');
  assert.equal(JSON.parse(calls[0].options.body).download_concurrency,10);
  assert.equal(input.dataset.dirty,'');
  assert.equal(input.value,'10');
  assert(fields['download-worker-summary'].textContent.includes('20'));
  assert(fields['stat-staging'].textContent.includes('8'));
  assert(fields['stat-staging'].textContent.includes('2.00'));
  assert(fields['stat-packing'].textContent.includes('3'));
  assert.equal(fields['stat-downloading'].textContent,'6 / 10 workers');
  assert.equal(fields['stat-preparing'].textContent,'2');
  const count=calls.length;
  for(const value of ['0','17','1.5','abc','']){
    input.value=value;await vm.runInContext('applyDownloadConcurrency()',context);
  }
  assert.equal(calls.length,count,'invalid controls must not issue requests');
  input.value='10';document.getElementById('harvest-pages').value='2';document.getElementById('harvest-source-mode').value='browser';
  document.getElementById('harvest-raid-difficulty').value='0';
  await vm.runInContext("startHarvest('raids')",context);
  const start=calls[calls.length-1];
  assert.equal(start.url,'/api/harvester/start');
  assert.equal(JSON.parse(start.options.body).download_concurrency,10);
  ok=false;response={detail:'Settings were not saved'};
  await vm.runInContext('applyDownloadConcurrency()',context);
  assert(fields['download-settings-feedback'].textContent.includes('Settings were not saved'));
  assert.equal(fields['btn-apply-download-threads'].disabled,false);
  vm.runInContext('renderHarvesterState({running:true})',context);
  assert.equal(input.disabled,true,'old servers must not pretend to apply the new setting');
  assert.equal(fields['btn-apply-download-threads'].disabled,true);
  assert(fields['download-settings-feedback'].textContent.includes('Restart'));
})().catch(error=>{console.error(error);process.exitCode=1});
'''
    result = subprocess.run([shutil.which('node') or 'node', '-e', script,
                             str(BASE_DIR / 'wcl_archiver/web/app.js')], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
