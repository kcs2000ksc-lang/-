'use strict';
// Public board headline reader: only ordinary HTTPS requests, no login, bypass, or media copying.
const cheerio=require('cheerio');
const SOURCE='https://m.fmkorea.com/pointplay';
const POST_RE=/^\/(\d{6,})(?:\/)?(?:[?#].*)?$/;
function clean(s,max=190){return String(s||'').replace(/\s+/g,' ').trim().slice(0,max);}
function extractTitle(title){
  let t=clean(title,250);
  const match=t.match(/^\[마감[^\]]*\]\s*/);
  const deadline=match?clean(match[0].slice(1,-1),45):'';
  t=t.replace(/^\[(?:마감|진행|오픈)[^\]]*\]\s*/,'').replace(/\[\d+\]\s*$/,'').trim();
  const parts=t.split(/\s*::\s*/);
  const sport=parts.length>=2?clean(parts[0],64):'잉토 경기';
  let selection=parts.length>=2?parts[1]:t;
  selection=selection.replace(/\s*#연장포함#\s*/g,' ').replace(/\s*#(?:정|우왕|갓)[^#]*#\s*/g,' ').trim();
  const vs=selection.match(/(.+?)\s+vs\.?\s+(.+)/i);
  const a=vs?clean(vs[1],80):clean(selection,80);
  const b=vs?clean(vs[2],80):'원문에서 확인';
  const schedule=parts.slice(2).join(' ').match(/\b(\d{1,2}\/\d{1,2})\s+(\d{1,2}:\d{2})\b/);
  const handicap=/[+\-]\d+(?:\.\d+)?/.test(selection);
  const market=/언\/?오버|언더|오버/i.test(selection)?'언더·오버':handicap?'핸디캡':'경기';
  return {sport,a,b,market,date:schedule?.[1]||'',time:schedule?.[2]||'',deadline};
}
function parsePointplay(html) {
  const $=cheerio.load(html);
  // Board title anchors from XenForo/XE-style list rows. Not site navigation or menus.
  const seen=new Set(), output=[];
  $('a[href]').each((_,node)=>{
    if(output.length>=45)return false;
    const $a=$(node);
    const href=$a.attr('href')||'';
    let u;
    try{u=new URL(href,SOURCE);}catch{return;}
    if(!['m.fmkorea.com','www.fmkorea.com','fmkorea.com'].includes(u.hostname))return;
    if(!POST_RE.test(u.pathname))return;
    const id=u.pathname.match(POST_RE)?.[1];
    if(!id||seen.has(id))return;
    let title=clean($a.text(),255);
    // Some list themes wrap title in a nested span.
    const wrapped=clean($a.find('.title, .subject, .item_title').first().text(),255);
    if(wrapped && wrapped.length>title.length/2)title=wrapped;
    title=title.replace(/\s*\[\d+\]\s*$/,'').trim();
    // Only pointplay match headings; ignore category controls and author links.
    if(!/(?:\bvs\.?\b|::)/i.test(title)||title.length<11)return;
    if(!/(?:경기|마감|VS|::|언\/오버|언더|오버)/i.test(title))return;
    const row=$a.closest('li, tr, article, .li, .board-item, .list-item');
    const comments=clean(row.find('.replyNum, .reply_num, .comment_count, .count').first().text(),12);
    const p=extractTitle(title);
    seen.add(id);
    output.push({
      id, sport:p.sport,a:p.a,b:p.b,date:p.date,time:p.time,
      market:p.market,comments:Math.max(0,Number(comments.replace(/[^0-9]/g,''))||0),
      desc:'에펨코리아 잉여력놀이터 게시글 제목입니다. 실시간 경기 스코어와 예측 확률은 제공하지 않습니다.',
      url:'https://m.fmkorea.com/'+id, title,deadline:p.deadline
    });
  });
  return output;
}
async function fetchPointplay(){
  const response=await fetch(SOURCE,{
    headers:{'Accept':'text/html,application/xhtml+xml','User-Agent':'GolmokIngtoBang/1.0 (public match headline reader)'},
    redirect:'follow',signal:AbortSignal.timeout(12000)
  });
  if(!response.ok){const e=new Error('HTTP '+response.status);e.status=response.status;throw e;}
  if(!response.headers.get('content-type')?.includes('text/html'))throw Error('원문 HTML을 받지 못했습니다');
  const length=Number(response.headers.get('content-length')||0);
  if(length>1500000)throw Error('원문 응답 용량 초과');
  const html=await response.text();
  if(html.length>2000000)throw Error('원문 응답 용량 초과');
  const matches=parsePointplay(html);
  if(!matches.length)throw Error('게시판 제목을 인식하지 못했습니다');
  return matches;
}
module.exports={SOURCE,parsePointplay,fetchPointplay,extractTitle};
