const express=require("express");
const http=require("http");
const {Server}=require("socket.io");
const crypto=require("crypto");
const app=express();
const server=http.createServer(app);
const io=new Server(server,{cors:{origin:"*"}});
app.use(express.static("public"));
const rooms=new Map();

const scenarios=[
{title:"ผลสัมฤทธิ์ลดลง",prompt:"ผลสัมฤทธิ์ของนักเรียนลดลงต่อเนื่อง 2 ภาคเรียน แต่ยังไม่มีข้อมูลเพียงพอที่จะระบุสาเหตุ",choices:["รวบรวมข้อมูลหลายด้านก่อน","เปิด PLC รับฟังทุกฝ่าย","ปรับวิธีสอนทันที","ตั้งมาตรการช่วยเหลือทันที"]},
{title:"ความขัดแย้งระหว่างครู",prompt:"ครูสองกลุ่มมีความเห็นไม่ตรงกันเรื่องภาระงานจนกระทบการทำงานร่วมกัน",choices:["เปิดพื้นที่รับฟังทุกฝ่าย","ผู้บริหารกำหนดใหม่ทันที","ให้หัวหน้ากลุ่มตัดสิน","ชะลอการตัดสินใจ"]},
{title:"ภาวะวิกฤตและความปลอดภัย",prompt:"ก่อนกิจกรรมกลางแจ้ง มีประกาศเตือนฝนตกหนักและอาจเกิดน้ำท่วม",choices:["ประเมินความเสี่ยงและปรับกิจกรรม","ยกเลิกทันที","ดำเนินตามแผนเดิม","รอดูสถานการณ์"]},
{title:"งบประมาณจำกัด",prompt:"งบไม่พอสำหรับ 2 โครงการสำคัญที่ต่างก็มีประโยชน์ต่อผู้เรียน",choices:["จัดลำดับตามผลกระทบและความจำเป็น","แบ่งงบให้ทั้งสอง","หางบเพิ่ม","ชะลอทั้งสอง"]},
{title:"การนำ AI มาใช้",prompt:"ครูต้องการใช้ AI แต่บางคนกังวลเรื่องความถูกต้อง ความปลอดภัย และการพึ่งพาเทคโนโลยี",choices:["ทดลองใช้ในวงจำกัดพร้อมแนวทางกำกับ","ใช้ทันที","ยังไม่อนุญาต","อบรมก่อนแล้วค่อยทดลอง"]},
{title:"ผู้ปกครองไม่เห็นด้วย",prompt:"โรงเรียนปรับรูปแบบการบ้าน แต่ผู้ปกครองบางส่วนไม่เห็นด้วยและแสดงความคิดเห็นออนไลน์",choices:["รับฟังข้อมูลและร่วมทบทวน","ยืนยันนโยบายเดิม","ยกเลิกทันที","ตั้งคณะทำงานร่วม"]}
];

function newRoom(){
 let r; do{r=crypto.randomBytes(3).toString("hex").toUpperCase()}while(rooms.has(r));
 rooms.set(r,{host:null,hostName:"",students:new Map(),scenario:0,started:false,responses:new Map()});
 return r;
}
function emitRoom(room,event,data){io.to(room).emit(event,data)}
function snapshot(r){return {count:r.students.size,responses:[...r.responses.values()],scenario:r.scenario,started:r.started}}
io.on("connection",socket=>{
 socket.on("createRoom",({name})=>{
   const room=newRoom(),r=rooms.get(room);r.host=socket.id;r.hostName=(name||"อาจารย์").slice(0,60);
   socket.join(room);socket.emit("roomCreated",{room,joinUrl:`${process.env.PUBLIC_URL||""}/?room=${room}`,scenarios});
 });
 socket.on("joinRoom",({room,name})=>{
   room=String(room||"").toUpperCase();const r=rooms.get(room);
   if(!r)return socket.emit("errorMessage","ไม่พบห้องนี้ หรือห้องหมดอายุแล้ว");
   r.students.set(socket.id,{id:socket.id,name:(name||"ผู้เรียน").slice(0,50)});socket.join(room);
   socket.emit("joined",{room,scenario:r.scenario,started:r.started,data:scenarios[r.scenario]});
   emitRoom(room,"roomUpdate",snapshot(r));
 });
 socket.on("startRound",({room})=>{
   const r=rooms.get(room);if(!r||r.host!==socket.id)return;
   r.responses.clear();r.started=true;
   emitRoom(room,"roundStarted",{scenario:r.scenario,data:scenarios[r.scenario]});
   emitRoom(room,"roomUpdate",snapshot(r));
 });
 socket.on("nextRound",({room})=>{
   const r=rooms.get(room);if(!r||r.host!==socket.id)return;
   if(r.scenario>=scenarios.length-1){r.started=false;emitRoom(room,"gameFinished",{});return}
   r.scenario++;r.responses.clear();r.started=true;
   emitRoom(room,"roundStarted",{scenario:r.scenario,data:scenarios[r.scenario]});
   emitRoom(room,"roomUpdate",snapshot(r));
 });
 socket.on("submitAnswer",({room,choice,answer,reflection})=>{
   const r=rooms.get(room),s=r?.students.get(socket.id);if(!r||!s||!r.started)return;
   const item={id:socket.id,name:s.name,choice:Number(choice),answer:String(answer||"").slice(0,1500),reflection:String(reflection||"").slice(0,1000),scenario:r.scenario,at:new Date().toISOString()};
   r.responses.set(socket.id,item);socket.to(r.host).emit("newResponse",item);
   emitRoom(room,"roomUpdate",snapshot(r));
 });
 socket.on("hostState",({room})=>{
   const r=rooms.get(room);if(!r||r.host!==socket.id)return;socket.emit("hostState",{...snapshot(r),scenarios});
 });
 socket.on("disconnect",()=>{
   for(const [room,r] of rooms){
     if(r.host===socket.id){rooms.delete(room);emitRoom(room,"roomClosed",{});}
     else if(r.students.delete(socket.id)){r.responses.delete(socket.id);emitRoom(room,"roomUpdate",snapshot(r));}
   }
 });
});
app.get("/health",(req,res)=>res.json({ok:true,rooms:rooms.size}));
const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log("Decision Lab running on port "+PORT));
