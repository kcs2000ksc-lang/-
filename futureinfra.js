'use strict';
// Optional, opt-in third-party collector. Never runs without FI_KEY.
// It is not an official FMKorea API; pointplay support must be verified by the user.
const {extractTitle}=require('./pointplay');
const BASE='https://futureinfra.ai/v1/scrape';
function tidy(v,max=180){return String(v||'').replace(/<[^>]*>/g,' ').replace(/\s+/g,' ').trim().slice(0,max);}
async function request(url,opts={}){
  const key=process.env.FI_KEY;
  if(!key)throw Error('FI_KEY 환경변수가 없습니다');
  const res=await fetch(url,{...opts,headers:{
    Authorization:'Bearer '+key,Accept:'application/json',...(opts.body?{'Content-Type':'application/json'}:{})
  },signal:AbortSignal.timeout(15000)});
  if(!res.ok)throw Error('외부 수집 제공업체 HTTP '+res.status);
  const json=await res.json();
  if(!json||typeof json!=='object')throw Error('수집 제공업체 응답 오류');
  return json;
}
function convertItem(item){
  const rawUrl=String(item.url||item.link||item.postUrl||item.post_url||item.documentUrl||'');
  let u;
  try{u=new URL(rawUrl,'https://m.fmkorea.com/');}catch{return null;}
  if(!['m.fmkorea.com','www.fmkorea.com','fmkorea.com'].includes(u.hostname))return null;
  const id=u.pathname.match(/^\/(\d{6,})\/?$/)?.[1];
  if(!id)return null;
  const title=tidy(item.title||item.subject||item.postTitle||item.headline||'',255);
  if(!/\s+vs\.?\s+/i.test(title))return null;
  const m=extractTitle(title);
  if(!m.a || !m.b || m.b==='원문에서 확인')return null;
  return {
    id, sport:m.sport,a:m.a,b:m.b,market:m.market,
    date:m.date,time:m.time,
    comments:Number(item.commentCount||item.comment_count||0),
    desc:'제3자 수집업체에서 제공한 잉토 게시글 제목입니다. 실제 스코어 및 적중 확률과는 다릅니다.',
    url:'https://m.fmkorea.com/'+id,
    title,deadline:m.deadline||''
  };
}
async function fetchFutureInfra(){
  if(!process.env.FI_KEY)throw Error('FI_KEY를 설정하지 않았습니다');
  const job=await request(BASE+'/runs',{
    method:'POST',
    body:JSON.stringify({
      actor:'store:community-fmkorea',
      input:{boards:['pointplay'],maxPosts:45,includeContent:false,includeComments:false}
    })
  });
  if(!/^[\w-]{8,}$/.test(String(job.id||'')))throw Error('수집 작업 ID를 받지 못했습니다');
  let status=job;
  for(let i=0;i<18;i++){
    if(status.status==='succeeded')break;
    if(['failed','aborted'].includes(status.status))throw Error('외부 수집 실패 ('+status.status+')');
    await new Promise(resolve=>setTimeout(resolve,4000));
    status=await request(BASE+'/runs/'+encodeURIComponent(job.id));
  }
  if(status.status!=='succeeded')throw Error('수집 작업 시간 초과. 실행 내역을 확인해 주세요');
  const results=await request(BASE+'/runs/'+encodeURIComponent(job.id)+'/items?limit=70');
  const items=Array.isArray(results.data)?results.data:Array.isArray(results.items)?results.items:[];
  const used=new Set(),matches=[];
  for(const item of items){
    const m=convertItem(item);
    if(m&&!used.has(m.id)){used.add(m.id);matches.push(m);}
  }
  if(!matches.length)throw Error('pointplay 게시판을 지원하지 않거나 경기 제목을 찾지 못했습니다');
  return matches.slice(0,45);
}
module.exports={fetchFutureInfra,convertItem};
