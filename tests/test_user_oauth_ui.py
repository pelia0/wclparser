import shutil
import subprocess

from wcl_archiver.config import BASE_DIR


def test_account_oauth_controls_and_loopback_navigation():
    script = r'''
const fs=require('fs'), vm=require('vm'), assert=require('assert');
const fields={}, opened=[], calls=[];
const document={getElementById(id){return fields[id] ||= {disabled:false,textContent:'',value:'',style:{},href:'',hidden:true};}};
let reply={has_user_oauth_token:true};
const context={document,console,URL,URLSearchParams,Date,
  setInterval(){return 1},clearInterval(){},
  window:{addEventListener(){},location:{origin:'http://127.0.0.1:8080',port:'8080'},
    open(...args){opened.push(args)}},
  fetch:async(url,options)=>{calls.push({url,options});return {ok:true,json:async()=>reply}}};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[1],'utf8'),context);
(async()=>{
 vm.runInContext('renderAuthState({authenticated:true,has_client_credentials:true,has_oauth_token:true,has_user_oauth_token:false})',context);
 assert.equal(fields['btn-oauth-connect'].disabled,false);
 assert.equal(fields['btn-oauth-disconnect'].disabled,true);
 assert(fields['auth-user-api-status'].textContent.includes('Not connected'));
 document.getElementById('auth-redirect-uri').value='http://localhost:8080/';
 vm.runInContext('connectUserOAuth()',context);
 const target=new URL(opened[0][0]);
 assert.equal(target.origin,'http://localhost:8080');
 assert.equal(target.pathname,'/api/auth/oauth/start');
 assert.equal(target.searchParams.get('redirect_uri'),'http://localhost:8080/');
 assert(opened[0][2].includes('noopener'));
 vm.runInContext('renderAuthState({authenticated:true,has_client_credentials:true,has_user_oauth_token:true,oauth_mode:"user"})',context);
 assert(fields['auth-user-api-status'].textContent.includes('Connected'));
 assert.equal(fields['btn-oauth-check'].disabled,false);
 reply={api_endpoint:'/api/v2/user',quota:{limitPerHour:3000},report:{isArchived:true,isAccessible:true}};
 await vm.runInContext('checkUserOAuthAccess()',context);
 assert.equal(calls[calls.length-1].url,'/api/auth/oauth/check');
 assert(fields['oauth-feedback'].textContent.includes('3000'));
 assert(fields['oauth-feedback'].textContent.includes('accessible'));
 reply={has_oauth_token:true,has_user_oauth_token:false};
 await vm.runInContext('disconnectUserOAuth()',context);
 assert.equal(calls[calls.length-1].url,'/api/auth/oauth/disconnect');
 assert.equal(fields['btn-oauth-check'].disabled,true);
 vm.runInContext('renderAuthState({has_client_credentials:true,has_user_oauth_token:false,oauth_mode:"user"})',context);
 assert.equal(fields['btn-oauth-disconnect'].disabled,false,'failed user authorization must still be disconnectable');
 assert(fields['auth-user-api-status'].textContent.includes('reconnect'));
})().catch(e=>{console.error(e);process.exitCode=1});
'''
    result = subprocess.run([shutil.which('node') or 'node', '-e', script, str(BASE_DIR / 'wcl_archiver/web/app.js')],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
