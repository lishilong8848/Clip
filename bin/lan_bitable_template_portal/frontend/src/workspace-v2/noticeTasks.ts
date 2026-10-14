import type {Dict} from './client';
export function noticeTasks(user:string):Dict[]{
  if(!user)return [];
  try{const rows=JSON.parse(sessionStorage.getItem('v2-notice-jobs:'+user)||'[]');return Array.isArray(rows)?rows.filter(r=>r&&typeof r.job_id==='string'&&Date.now()-r.at<7*86400000).slice(0,100):[];}catch{return [];}
}
export function rememberNoticeTask(user:string,job:Dict,notice:Dict){
  if(!user||!job.job_id)return;
  const rows=noticeTasks(user).filter(r=>r.job_id!==job.job_id);
  rows.unshift({job_id:job.job_id,title:String(notice.title||''),work_type:notice.work_type,scope:notice.scope,at:Date.now()});
  try{sessionStorage.setItem('v2-notice-jobs:'+user,JSON.stringify(rows.slice(0,100)));}catch{/* The server job remains authoritative when browser storage is unavailable. */}
}
