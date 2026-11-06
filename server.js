'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
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
let feedError = '';
let feedLastAttempt=0;
let nextAllowedAttempt=0;
function safeText(x, max=120) {return String(x ?? '').replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,max);}
function safeJson(x) {try {return JSON.parse(x);} catch {return null;}}
function send(ws, obj) {if(ws.readyState===WebSocket.OPEN) try{ws.send(JSON.stringify(obj));}catch{}}
function broadcast(obj) {const msg=JSON.stringify(obj); for(const ws of clients.keys())if(ws.readyState===WebSocket.OPEN)try{ws.send(msg);}catch{}}
function occupancy() {return [...clients.values()].filter(c=>c.joined).length;}
function listPlayers() {return [...clients.values()].filter(c=>c.joined).map(c=>({id:c.id,nick:c.nick,x:c.x,z:c.z,r:c.r,walking:c.walking}));}
function normalizeMatch(m, i) {
  if(!m || typeof m !== 'object')return null;
  const a=safeText(m.a||m.home,80),b=safeText(m.b||m.away,80);
  if(!a||!b)return null;
  const u=String(m.url||'');
  return {id:safeText(m.id||'match-'+i,80),sport:safeText(m.sport||'경기',90),
    a,b,date:safeText(m.date,24),time:safeText(m.time,30),market:safeText(m.market||'승패',80),
    comments:Math.max(0,Number(m.comments)||0),
    desc:safeText(m.desc||'실제 분석 자료가 없습니다.',240),
    url:/^https:\/\/(?:www\.)?fmkorea\.com\//i.test(u)?u:''};
}
async function refreshFeed(force=false) {
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
    if([403,429].includes(err.status))nextAllowedAttempt=Date.now()+60*60*1000;
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
    res.end(JSON.stringify({matches:feedLive?currentMatches:[],live:feedLive,source:feedSource,sourceUrl:POINTPLAY_URL,updatedAt:feedUpdatedAt,error:feedError,refreshMinutes:10}));
    return;
  }
  if(url.pathname==='/api/admin/events' && req.method==='POST'){
    if(!process.env.ADMIN_TOKEN || req.headers.authorization!=='Bearer '+process.env.ADMIN_TOKEN) {
      res.writeHead(403);res.end('forbidden');return;
    }
    let chunks='',size=0,aborted=false;
    req.on('data',chunk=>{
      size+=chunk.length;
      if(size>300000){aborted=true;req.destroy();return;}
      chunks+=chunk;
    });
    req.on('end',()=>{
      if(aborted)return;
      const raw=safeJson(chunks), list=Array.isArray(raw)?raw:raw?.matches;
      if(!Array.isArray(list)){res.writeHead(400);res.end('bad json');return;}
      currentMatches=list.slice(0,45).map(normalizeMatch).filter(Boolean);
      feedLive=true;feedSource='관리자 등록 경기표';feedUpdatedAt=new Date().toISOString();
      broadcast({t:'feed_updated',updatedAt:feedUpdatedAt});
      res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true,count:currentMatches.length}));
    });
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
refreshFeed();
server.listen(PORT,'0.0.0.0',()=>console.log('Metaverse listening on '+PORT));
