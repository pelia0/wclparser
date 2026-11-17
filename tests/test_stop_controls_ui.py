"""Run the real UI against deferred network boundaries; no live server is touched."""
import shutil
import subprocess

from wcl_archiver.config import BASE_DIR


HARNESS = r'''
const fs=require('fs'), vm=require('vm'), assert=require('assert');
class Element {
  constructor(){this.value='';this.textContent='';this.style={};this.dataset={};this.disabled=false;}
  setCustomValidity(){} reportValidity(){return true;} addEventListener(){}
}
const fields={}, calls=[];
const document={activeElement:null,getElementById(id){return fields[id] ||= new Element();}};
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});return {promise,resolve,reject};}
const response=(state,ok=true)=>({ok,status:ok?200:503,json:async()=>state});
let network=()=>Promise.resolve(response({running:true}));
const timers=new Map();let timerId=0;
function abortable(promise,signal){
  if(!signal)return promise;
  return new Promise((resolve,reject)=>{
    const aborted=()=>{const error=new Error('Fetch aborted');error.name='AbortError';reject(error);};
    if(signal.aborted){aborted();return;}
    signal.addEventListener('abort',aborted,{once:true});
    promise.then(resolve,reject).finally(()=>signal.removeEventListener('abort',aborted));
  });
}
const context={document,window:{addEventListener(){}},console,URLSearchParams,AbortController,
 setTimeout(callback,delay){const id=++timerId;timers.set(id,{callback,delay});return id;},
 clearTimeout(id){timers.delete(id);},
 fetch:(url,options)=>{calls.push({url,options});return abortable(network(url,options),options?.signal);}};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[1],'utf8'),context);
const run=code=>vm.runInContext(code,context);
const state={running:true,stopping:false,supports_async_stop:true,current_task:'Downloading',
 download_concurrency:32,browser_request_concurrency:64,pack_workers:4,pages_scanned:123,
 queued_fights:456,packed_fights:789,compressed_mb:55,downloading_fights:20,staging_files:9};
function render(value=state){context.fixture=value;run('renderHarvesterState(fixture)');}
function assertLocked(){for(const id of ['btn-harvest-stop','btn-harvest-mplus','btn-harvest-raids','btn-import-report']) assert.equal(document.getElementById(id).disabled,true,id);}
'''


def run_ui(body):
    script = HARNESS + "\n(async()=>{\n" + body + r'''
})().catch(error=>{console.error(error);process.exitCode=1});
'''
    result = subprocess.run(
        [shutil.which('node') or 'node', '-e', script,
         str(BASE_DIR / 'wcl_archiver/web/app.js')], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_status_requests_coalesce_and_recover_after_http_and_network_errors():
    run_ui(r'''
render();
let pending=deferred();network=()=>pending.promise;
const first=run('refreshHarvesterStatus()');
const second=run('refreshHarvesterStatus()');
const third=run('refreshHarvesterStatus()');
assert.equal(calls.length,1,'timer and direct refresh callers must share the pending request');
pending.resolve(response({detail:'Status unavailable'},false));
await Promise.all([first,second,third]);
assert.equal(fields['stat-state'].textContent,'RUNNING','HTTP error is not an idle status');
assert(fields['harvester-error-banner'].textContent.includes('Status unavailable'));
network=()=>Promise.reject(new Error('Network unavailable'));
await run('refreshHarvesterStatus()');
assert(fields['harvester-error-banner'].textContent.includes('Network unavailable'));
network=()=>Promise.resolve(response({...state,pages_scanned:124}));
await run('refreshHarvesterStatus()');
assert.equal(calls.length,3);
assert.equal(fields['stat-pages'].textContent,'124');
assert.equal(fields['harvester-error-banner'].style.display,'none');
''')


def test_stop_feedback_is_immediate_and_partial_ack_preserves_counters_and_rejects_stale_poll():
    run_ui(r'''
render();
const oldPoll=deferred(),stop=deferred();
network=url=>url.includes('/stop')?stop.promise:oldPoll.promise;
const poll=run('refreshHarvesterStatus()');
const request=run('stopHarvest()');
assertLocked();
assert.equal(fields['stat-state'].textContent,'STOPPING');
assert(fields['btn-harvest-stop'].textContent.includes('Stop requested'));
assert(fields['harvest-task-label'].textContent.includes('Stop requested'));
assert.equal(calls[1].url,'/api/harvester/stop?wait=false');
await run('stopHarvest()');
await run("startHarvest('mplus_s5')");
await run('importSingleReport()');
assert.equal(calls.length,2,'duplicate stop and competing actions must not issue requests');
stop.resolve(response({running:false,stopping:true,current_task:'Saving snapshots',stop_request_accepted:true}));
await request;
assertLocked();
assert.equal(fields['stat-state'].textContent,'STOPPING');
assert.equal(fields['harvest-task-label'].textContent,'Saving snapshots');
assert.equal(fields['stat-pages'].textContent,'123');
assert.equal(fields['stat-queued'].textContent,'456');
assert.equal(fields['stat-packed'].textContent,'789');
assert.equal(fields['stat-downloading'].textContent,'20 / 32 workers');
assert.equal(fields['harvest-download-threads'].disabled,false,'partial ack must preserve live worker controls');
oldPoll.resolve(response({...state,pages_scanned:1}));await poll;
assertLocked();assert.equal(fields['stat-pages'].textContent,'123');
network=()=>Promise.resolve(response({...state,running:false,stopping:false,current_task:'Stopped'}));
await run('refreshHarvesterStatus()');
assert.equal(fields['stat-state'].textContent,'IDLE');
assert.equal(fields['btn-harvest-mplus'].disabled,false);
assert.equal(fields['btn-import-report'].disabled,false);
assert.equal(fields['btn-harvest-stop'].textContent,'Stop Harvester');
render({running:true});
assert.equal(fields['harvest-download-threads'].disabled,true,'ordinary partial old-server status must not inherit capabilities');
''')


def test_legacy_stop_waits_with_feedback_and_failed_stop_can_retry_without_fake_idle():
    run_ui(r'''
render({...state,supports_async_stop:undefined});
let stop=deferred();network=()=>stop.promise;
let request=run('stopHarvest()');assertLocked();
assert.equal(calls[0].url,'/api/harvester/stop');
assert(fields['harvest-task-label'].textContent.includes('Stop requested'));
stop.resolve(response({detail:'Stop unavailable'},false));await request;
assert.equal(fields['stat-state'].textContent,'RUNNING');
assert.equal(fields['btn-harvest-stop'].disabled,false);
assert(fields['harvester-error-banner'].textContent.includes('Stop unavailable'));
network=()=>Promise.reject(new Error('Stop connection lost'));
await run('stopHarvest()');
assert.equal(fields['btn-harvest-stop'].disabled,false);
assert.equal(fields['stat-state'].textContent,'RUNNING');
assert(fields['harvester-error-banner'].textContent.includes('Stop connection lost'));
stop=deferred();network=()=>stop.promise;
request=run('stopHarvest()');assertLocked();
stop.resolve(response({...state,supports_async_stop:undefined,running:false,stopping:false,current_task:'Stopped'}));
await request;
assert.equal(fields['stat-state'].textContent,'IDLE');
assert.equal(fields['btn-harvest-stop'].disabled,false);
assert.equal(fields['btn-import-report'].disabled,false);
''')


def test_pending_legacy_stop_keeps_controls_locked_across_poll_and_import_completion():
    run_ui(r'''
render({...state,supports_async_stop:undefined});
fields['import-report-input']=new Element();fields['import-report-input'].value='ABC';
const importing=deferred(),stop=deferred();
network=url=>url.includes('/import-report')?importing.promise:url.includes('/stop')?stop.promise:Promise.resolve(response({...state,running:false,stopping:false}));
const imported=run('importSingleReport()');
const request=run('stopHarvest()');assertLocked();
await run('refreshHarvesterStatus()');assertLocked();
importing.resolve(response({detail:'Import cancelled'},false));await imported;
assertLocked();
stop.resolve(response({...state,running:false,stopping:false}));await request;
assert.equal(fields['btn-import-report'].disabled,false);
''')


def test_async_ack_requires_a_fresh_poll_even_if_ack_reports_cleanup_already_finished():
    run_ui(r'''
render();
const pendingStop=deferred(),pendingPoll=deferred();
network=url=>url.includes('/stop')?pendingStop.promise:pendingPoll.promise;
const stop=run('stopHarvest()');
const poll=run('refreshHarvesterStatus()');
pendingStop.resolve(response({running:false,stopping:false,current_task:'Stopped',stop_request_accepted:true}));
await stop;assertLocked();
pendingPoll.resolve(response({...state,running:true,current_task:'Old download snapshot'}));
await poll;assertLocked();
assert.equal(fields['stat-state'].textContent,'STOPPING');
assert(!fields['harvest-task-label'].textContent.includes('Old download snapshot'));
network=()=>Promise.resolve(response({...state,running:false,stopping:false}));
await run('refreshHarvesterStatus()');
assert.equal(fields['stat-state'].textContent,'IDLE');
assert.equal(fields['btn-harvest-stop'].disabled,false);
''')


def test_stop_aborts_outdated_polls_and_old_finally_cannot_release_a_new_poll():
    run_ui(r'''
render();
const stopping=deferred(),polls=[];
network=url=>{if(url.includes('/stop'))return stopping.promise;const pending=deferred();polls.push(pending);return pending.promise;};
const old=run('refreshHarvesterStatus()');
const stop=run('stopHarvest()');
assert.equal(calls[0].options?.signal?.aborted,true,'Stop must abort the outage poll immediately');
const middle=run('refreshHarvesterStatus()');
assert.equal(polls.length,2,'a new status request must not wait for the aborted poll');
await old;
const sharedMiddle=run('refreshHarvesterStatus()');
assert.equal(polls.length,2,'old finally must not clear ownership of the newer request');
stopping.resolve(response({running:false,stopping:true,stop_request_accepted:true}));await stop;
assert.equal(calls[2].options.signal.aborted,true,'acknowledgement must invalidate a poll started during Stop');
const fresh=run('refreshHarvesterStatus()');
assert.equal(polls.length,3);
await Promise.all([middle,sharedMiddle]);
const sharedFresh=run('refreshHarvesterStatus()');
assert.equal(polls.length,3,'the middle finally must not release the fresh request');
polls[2].resolve(response({...state,running:false,stopping:false,current_task:'Stopped'}));
await Promise.all([fresh,sharedFresh]);
assert.equal(fields['stat-state'].textContent,'IDLE');
assert.equal(fields['btn-harvest-mplus'].disabled,false);
assert.equal(fields['harvester-error-banner'].style.display,'none','stale AbortError must remain silent');
assert.equal(timers.size,0,'settled and invalidated status requests must clear their timers');
''')


def test_status_timeout_aborts_hanging_fetch_and_allows_a_clean_retry():
    run_ui(r'''
render();network=()=>deferred().promise;
const first=run('refreshHarvesterStatus()'),shared=run('refreshHarvesterStatus()');
assert.equal(calls.length,1);
assert.equal(timers.size,1,'each shared status request needs one bounded timeout');
const [id,timer]=[...timers][0];
assert(timer.delay>0&&timer.delay<=30000,'an outage must not block future polls indefinitely');
timers.delete(id);timer.callback();
await Promise.all([first,shared]);
assert.equal(calls[0].options.signal.aborted,true);
assert.equal(fields['stat-state'].textContent,'RUNNING');
assert(/timed out/i.test(fields['harvester-error-banner'].textContent));
const retry=deferred();network=()=>retry.promise;
const refreshed=run('refreshHarvesterStatus()'),sharedRetry=run('refreshHarvesterStatus()');
assert.equal(calls.length,2,'retry must own a new coalesced request after timeout');
retry.resolve(response({...state,pages_scanned:130}));
await Promise.all([refreshed,sharedRetry]);
assert.equal(fields['stat-pages'].textContent,'130');
assert.equal(fields['harvester-error-banner'].style.display,'none');
assert.equal(timers.size,0);
''')
