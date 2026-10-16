// One mock server shared by independent browser contexts; no production data.
export function layoutApiFixture(){
  const records=new Map();
  let writes=0;
  async function handle(route,owner){
    const request=route.request(),url=new URL(request.url());
    if(!url.pathname.startsWith('/api/workspace-layouts/'))return false;
    const key=owner+':'+url.pathname,current=records.get(key)||{revision:0,value:null};
    if(request.method()==='GET')await route.fulfill({json:{ok:true,data:current}});
    else{
      const payload=request.postDataJSON();writes++;
      if(JSON.stringify(payload.value)===JSON.stringify(current.value))await route.fulfill({json:{ok:true,data:current}});
      else if(payload.revision!==current.revision)await route.fulfill({status:409,json:{ok:false,error:'另一台设备或页面已修改布局，请重新读取后再调整。'}});
      else{const next={revision:current.revision+1,value:payload.value};records.set(key,next);await route.fulfill({json:{ok:true,data:next}});}
    }
    return true;
  }
  return {records,handle,get writes(){return writes;}};
}
