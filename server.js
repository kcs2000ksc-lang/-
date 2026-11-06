'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { Pool } = require('pg');
const { WebSocketServer, WebSocket } = require('ws');
const { SOURCE:POINTPLAY_URL, fetchPointplay }=require('./pointplay');
const PORT = Number(process.env.PORT || 3000);
const MAX_USERS = 100;
const clients = new Map();
const history = [];
let idSeq = 0;
let currentMatches = [];
let feedUpdatedAt = null;
let feedSource = '사진 기반 예시';
let feedLive = false;
let boardMode = 'auto';
let db = null;
const LOCAL_SAVE = path.join('/tmp','ingto-managed-matches.json');
let feedError = '';
let feedLastAttempt=0;
let nextAllowedAttempt=0;
function safeText(x, max=120) {return String(x ?? '').replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,max);}
function safeJson(x) {try {return JSON.parse(x);} catch {return null;}}
function send(ws, obj) {if(ws.readyState===WebSocket.OPEN) try{ws.send(JSON.stringify(obj));}catch{}}
function broadcast(obj) {const msg=JSON.stringify(obj); for(const ws of clients.keys())if(ws.readyState===WebSocket.OPEN)try{ws.send(msg);}catch{}}
function occupancy() {return [...clients.values()].filter(c=>c.joined).length;}
function listPlayers() {return [...clients.values()].filter(c=>c.joined).map(c=>({id:c.id,nick:c.nick,x:c.x,z:c.z,r:c.r,walking:c.walking}));}
function isAdmin(req) {
  const expected=process.env.ADMIN_TOKEN||'';
  const auth=String(req.headers.authorization||'');
  if(expected.length<20 || !auth.startsWith('Bearer '))return false;
  const a=Buffer.from(auth.slice(7)),b=Buffer.from(expected);
  return a.length===b.length && crypto.timingSafeEqual(a,b);
}
function json(res,code,data){
  res.writeHead(code,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff'});
  res.end(JSON.stringify(data));
}
function validateAdminMatches(list){
  if(!Array.isArray(list)||list.length>45)return {error:'최대 45경기까지 등록할 수 있습니다.'};
  const result=[],ids=new Set();
  for(let i=0;i<list.length;i++){
    const m=list[i];
    if(!m||typeof m!=='object'||!safeText(m.sport,90)||!safeText(m.a,80)||!safeText(m.b,80))return {error:(i+1)+'번 경기의 종목·팀명이 비어 있습니다.'};
    const u=String(m.url||'').trim();
    if(u && !/^https:\/\/(?:m\.|www\.)?fmkorea\.com\/\d{6,}(?:[\/?#].*)?$/i.test(u))return {error:'게시글 주소는 에펨코리아 원문만 허용됩니다.'};
    const item=normalizeMatch({...m,url:u},i);
    item.id=safeText(m.id,70)||('manual-'+Date.now().toString(36)+'-'+i);
    if(ids.has(item.id))item.id='manual-'+Date.now().toString(36)+'-'+i;
    ids.add(item.id);
    result.push(item);
  }
  return {matches:result};
}
async function initializeStorage(){
  if(process.env.DATABASE_URL){
    try{
      db=new Pool({connectionString:process.env.DATABASE_URL,connectionTimeoutMillis:7000,max:2});
      await db.query('CREATE TABLE IF NOT EXISTS ingto_manual_board (id smallint PRIMARY KEY, matches jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())');
      const rs=await db.query('SELECT matches,updated_at FROM ingto_manual_board WHERE id=1');
      if(rs.rows.length){
        boardMode='manual';currentMatches=rs.rows[0].matches;
        feedLive=currentMatches.length>0;feedSource='관리자 직접 등록';
        feedUpdatedAt=rs.rows[0].updated_at.toISOString();
        console.log('Restored registered matches from database',currentMatches.length);
      }
    }catch(err){console.error('Database unavailable',err.message);if(db){await db.end().catch(()=>{});db=null;}}
  }
  if(!db) {
    try {
      const old=JSON.parse(fs.readFileSync(LOCAL_SAVE,'utf8'));
      if(Array.isArray(old.matches)){currentMatches=old.matches;boardMode='manual';feedLive=old.matches.length>0;feedSource='관리자 직접 등록';feedUpdatedAt=old.updatedAt||null;}
    }catch{}
  }
}
async function saveAdminMatches(matches){
  const updatedAt=new Date().toISOString();
  if(db){
    await db.query('INSERT INTO ingto_manual_board(id,matches,updated_at) VALUES (1,$1,$2) ON CONFLICT (id) DO UPDATE SET matches=$1,updated_at=$2',[JSON.stringify(matches),updatedAt]);
  } else {
    // Local file is best-effort only: Render free service filesystem is ephemeral.
    fs.writeFileSync(LOCAL_SAVE,JSON.stringify({matches,updatedAt}),'utf8');
  }
  currentMatches=matches;feedUpdatedAt=updatedAt;
  feedLive=matches.length>0;boardMode='manual';feedSource='관리자 직접 등록';
  feedError='';
  broadcast({t:'feed_updated',updatedAt,source:'admin',count:matches.length});
  return updatedAt;
}
function normalizeMatch(m, i) {
  if(!m || typeof m !== 'object')return null;
  const a=safeText(m.a||m.home,80),b=safeText(m.b||m.away,80);
  if(!a||!b)return null;
  const u=String(m.url||'');
  return {id:safeText(m.id||'match-'+i,80),sport:safeText(m.sport||'경기',90),
    a,b,date:safeText(m.date,24),time:safeText(m.time,30),market:safeText(m.market||'승패',80),
    comments:Math.max(0,Number(m.comments)||0),
    desc:safeText(m.desc||'실제 분석 자료가 없습니다.',240),
    url:/^https:\/\/(?:m\.|www\.)?fmkorea\.com\/\d{6,}/i.test(u)?u:''};
}
async function refreshFeed(force=false) {
  if(boardMode==='manual')return;
  const now=Date.now();
  if(now<nextAllowedAttempt || now-feedLastAttempt<(force?180000:450000))return;
  feedLastAttempt=now;
  const url=process.env.EVENTS_FEED_URL;
  try {
    let list;
    if(url && /^https:\/\//i.test(url)) {
      const response=await fetch(url,{signal:AbortSignal.timeout(12000),headers:{Accept:'application/json'}});
      if(!response.ok){const e=new Error('feed HTTP '+response.status);e.status=response.status;throw e;}
      const raw=await response.json();
      list=Array.isArray(raw)?raw:raw.matches;
      if(!Array.isArray(list))throw Error('invalid feed format');
      feedSource='승인된 외부 경기 피드';
    } else {
      list=await fetchPointplay();
      feedSource='에펨코리아 잉여력놀이터 원문';
    }
    const out=list.slice(0,45).map(normalizeMatch).filter(Boolean);
    if(!out.length)throw Error('받아온 경기 게시글이 없습니다');
    currentMatches=out;
    feedLive=true; feedError='';
    feedUpdatedAt=new Date().toISOString();
    broadcast({t:'feed_updated',updatedAt:feedUpdatedAt});
    console.log('match headlines updated',out.length,feedSource);
  } catch(err) {
    feedLive=false;
    feedError=String(err.message||'조회 실패').slice(0,130);
    if([403,429,430].includes(err.status))nextAllowedAttempt=Date.now()+60*60*1000;
    feedSource='에펨코리아 잉여력놀이터 · 조회 제한 또는 실패';
    console.warn('match headlines unavailable',feedError);
  }
}
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');
  if(url.pathname==='/api/health'){
    res.writeHead(200,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});
    res.end(JSON.stringify({ok:true,users:occupancy(),capacity:MAX_USERS,liveFeed:feedLive}));
    return;
  }
  if(url.pathname==='/api/events'){
    if(url.searchParams.get('refresh')==='1')refreshFeed(true).catch(()=>{});
    res.writeHead(200,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});
    res.end(JSON.stringify({matches:feedLive?currentMatches:[],live:feedLive,manual:boardMode==='manual',source:feedSource,sourceUrl:POINTPLAY_URL,updatedAt:feedUpdatedAt,error:feedError,refreshMinutes:10}));
    return;
  }
  if(url.pathname==='/api/admin/events'){
    if(!isAdmin(req)){json(res,403,{message:'관리자 암호가 올바르지 않습니다.'});return;}
    if(req.method==='GET'){
      json(res,200,{matches:boardMode==='manual'?currentMatches:[],updatedAt:feedUpdatedAt,source:feedSource,persistent:!!db,mode:boardMode});
      return;
    }
    if(req.method!=='POST'){json(res,405,{message:'지원하지 않는 요청입니다.'});return;}
    let body='',size=0,tooLarge=false;
    req.on('data',chunk=>{
      size+=chunk.length;
      if(size>120000){tooLarge=true;req.destroy();return;}
      body+=chunk;
    });
    req.on('end',async()=>{
      if(tooLarge)return;
      const raw=safeJson(body),validated=validateAdminMatches(raw?.matches);
      if(validated.error){json(res,400,{message:validated.error});return;}
      try{
        const updatedAt=await saveAdminMatches(validated.matches);
        json(res,200,{ok:true,matches:currentMatches,updatedAt,persistent:!!db});
      }catch(err){console.error('Admin save failed',err.message);json(res,503,{message:'경기 목록을 저장하지 못했습니다.'});}
    });
    return;
  }
  if(req.method==='GET'&&['/admin','/admin.html'].includes(url.pathname)){
    res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store',
      'x-content-type-options':'nosniff','x-frame-options':'DENY',
      'content-security-policy':"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'"});
    fs.createReadStream(path.join(__dirname,'admin.html')).pipe(res);
    return;
  }
  if(req.method!=='GET'||!['/','/index.html'].includes(url.pathname)){res.writeHead(404);res.end('Not Found');return;}
  res.writeHead(200,{
    'content-type':'text/html; charset=utf-8',
    'cache-control':'public, max-age=120',
    'x-content-type-options':'nosniff',
    'content-security-policy':"default-src 'self' https://cdn.jsdelivr.net; script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss: https://cdn.jsdelivr.net; img-src 'self' data: blob:; font-src 'self' data:"
  });
  fs.createReadStream(path.join(__dirname,'index.html')).pipe(res);
});
const wss=new WebSocketServer({server,maxPayload:1600,perMessageDeflate:false});
wss.on('connection',ws=>{
  if(occupancy()>=MAX_USERS){send(ws,{t:'error',code:'full',message:'입장 인원이 100명으로 가득 찼습니다.'});ws.close(1008,'full');return;}
  const state={id:String(++idSeq),joined:false,nick:'',x:0,z:5.1,r:0,walking:false,lastChat:0,lastMove:0};
  clients.set(ws,state);ws.isAlive=true;
  ws.on('pong',()=>{ws.isAlive=true;});
  ws.on('message',data=>{
    const m=safeJson(data.toString());if(!m||typeof m!=='object')return;
    if(m.t==='join'&&!state.joined){
      if(occupancy()>=MAX_USERS){send(ws,{t:'error',code:'full',message:'방이 가득 찼습니다.'});return;}
      const name=safeText(m.nick,14);
      if(name.length<2){send(ws,{t:'error',code:'bad_name',message:'닉네임을 2~14자로 입력하세요.'});return;}
      if([...clients.values()].some(p=>p.joined&&p.nick.toLowerCase()===name.toLowerCase())){
        send(ws,{t:'error',code:'name_taken',message:'이미 사용 중인 닉네임입니다.'});return;
      }
      state.joined=true;state.nick=name;
      send(ws,{t:'welcome',id:state.id,nick:name,players:listPlayers(),history,capacity:MAX_USERS});
      broadcast({t:'notice',text:name+'님이 입장했습니다.',users:occupancy()});
      return;
    }
    if(!state.joined)return;
    if(m.t==='move'){
      const now=Date.now();if(now-state.lastMove<130)return;
      state.lastMove=now;
      if([m.x,m.z,m.r].every(Number.isFinite)){
        state.x=Math.max(-6.7,Math.min(6.7,m.x));
        state.z=Math.max(-6.2,Math.min(6.2,m.z));
        state.r=m.r;state.walking=!!m.walking;
      }
      return;
    }
    if(m.t==='chat'){
      const now=Date.now();if(now-state.lastChat<800)return;
      const message=safeText(m.text,160);if(!message)return;
      state.lastChat=now;
      const event={t:'chat',id:state.id,nick:state.nick,text:message,at:now};
      history.push(event);if(history.length>30)history.shift();broadcast(event);
      return;
    }
    if(m.t==='drink'&&['water','coffee'].includes(m.kind))broadcast({t:'activity',id:state.id,kind:m.kind});
  });
  ws.on('close',()=>{
    clients.delete(ws);
    if(state.joined)broadcast({t:'notice',text:state.nick+'님이 퇴장했습니다.',users:occupancy()});
  });
  ws.on('error',()=>{});
});
setInterval(()=>{const players=listPlayers();if(players.length)broadcast({t:'state',players,users:players.length});},350).unref();
setInterval(()=>{for(const ws of wss.clients){if(ws.isAlive===false){ws.terminate();continue;}ws.isAlive=false;ws.ping();}},30000).unref();
setInterval(refreshFeed,10*60*1000).unref();
(async()=>{await initializeStorage();await refreshFeed();server.listen(PORT,'0.0.0.0',()=>console.log('Metaverse listening on '+PORT));})().catch(err=>{console.error('Startup failure',err);process.exit(1)});
