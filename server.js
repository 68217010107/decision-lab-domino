const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: { origin: true, methods: ["GET", "POST"] },
  transports: ["polling", "websocket"],
  pingInterval: 25000,
  pingTimeout: 20000
});

app.set("trust proxy", 1);
app.use(express.json());
app.use(express.static("public"));

const rooms = new Map();

// ใช้ token แทน socket.id เพื่อให้สิทธิ์อาจารย์ไม่หายเมื่อมือถือ/เบราว์เซอร์ reconnect
function makeHostToken() { return crypto.randomBytes(24).toString("hex"); }

const scenarios = [
  {title:"ผลสัมฤทธิ์ลดลง",prompt:"ผลสัมฤทธิ์ของนักเรียนลดลงต่อเนื่อง 2 ภาคเรียน แต่ยังไม่มีข้อมูลเพียงพอที่จะระบุสาเหตุ",
   choices:["รวบรวมข้อมูลหลายด้านก่อน","เปิด PLC รับฟังทุกฝ่าย","ปรับวิธีสอนทันที","ตั้งมาตรการช่วยเหลือทันที"]},
  {title:"ความขัดแย้งระหว่างครู",prompt:"ครูสองกลุ่มมีความเห็นไม่ตรงกันเรื่องภาระงานจนกระทบการทำงานร่วมกัน",
   choices:["เปิดพื้นที่รับฟังทุกฝ่าย","ผู้บริหารกำหนดใหม่ทันที","ให้หัวหน้ากลุ่มตัดสิน","ชะลอการตัดสินใจ"]},
  {title:"ภาวะวิกฤตและความปลอดภัย",prompt:"ก่อนกิจกรรมกลางแจ้ง มีประกาศเตือนฝนตกหนักและอาจเกิดน้ำท่วม",
   choices:["ประเมินความเสี่ยงและปรับกิจกรรม","ยกเลิกทันที","ดำเนินตามแผนเดิม","รอดูสถานการณ์"]},
  {title:"งบประมาณจำกัด",prompt:"งบไม่พอสำหรับ 2 โครงการสำคัญที่ต่างก็มีประโยชน์ต่อผู้เรียน",
   choices:["จัดลำดับตามผลกระทบและความจำเป็น","แบ่งงบให้ทั้งสอง","หางบเพิ่ม","ชะลอทั้งสอง"]},
  {title:"การนำ AI มาใช้",prompt:"ครูต้องการใช้ AI แต่บางคนกังวลเรื่องความถูกต้อง ความปลอดภัย และการพึ่งพาเทคโนโลยี",
   choices:["ทดลองใช้ในวงจำกัดพร้อมแนวทางกำกับ","ใช้ทันที","ยังไม่อนุญาต","อบรมก่อนแล้วค่อยทดลอง"]},
  {title:"ผู้ปกครองไม่เห็นด้วย",prompt:"โรงเรียนปรับรูปแบบการบ้าน แต่ผู้ปกครองบางส่วนไม่เห็นด้วยและแสดงความคิดเห็นออนไลน์",
   choices:["รับฟังข้อมูลและร่วมทบทวน","ยืนยันนโยบายเดิม","ยกเลิกทันที","ตั้งคณะทำงานร่วม"]}
];

function makeRoomCode() {
  let code;
  do { code = crypto.randomBytes(3).toString("hex").toUpperCase(); }
  while (rooms.has(code));
  return code;
}

function roomSnapshot(r) {
  return {
    count: r.students.size,
    responses: [...r.responses.values()],
    scenario: r.scenario,
    started: r.started
  };
}

function emitRoom(room, event, payload) {
  io.to(room).emit(event, payload);
}

io.on("connection", (socket) => {
  socket.emit("serverReady", { ok: true, socketId: socket.id });

  socket.on("createRoom", ({ name }) => {
    try {
      const room = makeRoomCode();
      const r = {
        host: socket.id,
        hostToken: makeHostToken(),
        hostName: String(name || "อาจารย์").slice(0, 60),
        students: new Map(),
        scenario: 0,
        started: false,
        responses: new Map()
      };
      rooms.set(room, r);
      socket.join(room);
      socket.data.role = "host";
      socket.data.room = room;
      socket.data.hostToken = r.hostToken;

      socket.emit("roomCreated", {
        room,
        joinUrl: `${process.env.RENDER_EXTERNAL_URL || ""}/?room=${room}`,
        scenarios,
        hostToken: r.hostToken
      });
    } catch (err) {
      socket.emit("serverError", "สร้างห้องไม่สำเร็จ: " + err.message);
    }
  });

  socket.on("hostRejoin", ({ room, hostToken }) => {
    room = String(room || "").trim().toUpperCase();
    const r = rooms.get(room);
    if (!r || !hostToken || hostToken !== r.hostToken) {
      return socket.emit("serverError", "ไม่สามารถยืนยันสิทธิ์อาจารย์ของห้องนี้ได้");
    }
    r.host = socket.id;
    socket.join(room);
    socket.data.role = "host";
    socket.data.room = room;
    socket.data.hostToken = hostToken;
    socket.emit("hostRejoined", { room, scenarios, scenario: r.scenario, started: r.started, data: scenarios[r.scenario] });
    socket.emit("roomUpdate", roomSnapshot(r));
  });

  socket.on("joinRoom", ({ room, name }) => {
    room = String(room || "").trim().toUpperCase();
    const r = rooms.get(room);
    if (!r) return socket.emit("serverError", "ไม่พบห้องนี้ หรือห้องหมดอายุแล้ว");

    const student = { id: socket.id, name: String(name || "ผู้เรียน").slice(0, 50) };
    r.students.set(socket.id, student);
    socket.join(room);
    socket.data.role = "student";
    socket.data.room = room;

    socket.emit("joined", {
      room,
      scenario: r.scenario,
      started: r.started,
      data: scenarios[r.scenario]
    });
    emitRoom(room, "roomUpdate", roomSnapshot(r));
  });

  socket.on("startRound", ({ room, hostToken }) => {
    const r = rooms.get(String(room || "").toUpperCase());
    if (!r || r.host !== socket.id || hostToken !== r.hostToken) return socket.emit("serverError", "ไม่มีสิทธิ์เริ่มกิจกรรม");
    r.responses.clear();
    r.started = true;
    emitRoom(room, "roundStarted", { scenario: r.scenario, data: scenarios[r.scenario] });
    emitRoom(room, "roomUpdate", roomSnapshot(r));
  });

  socket.on("nextRound", ({ room, hostToken }) => {
    const r = rooms.get(String(room || "").toUpperCase());
    if (!r || r.host !== socket.id || hostToken !== r.hostToken) return socket.emit("serverError", "ไม่มีสิทธิ์เปลี่ยนสถานการณ์");
    if (r.scenario >= scenarios.length - 1) {
      r.started = false;
      emitRoom(room, "gameFinished", {});
      emitRoom(room, "roomUpdate", roomSnapshot(r));
      return;
    }
    r.scenario += 1;
    r.responses.clear();
    r.started = true;
    emitRoom(room, "roundStarted", { scenario: r.scenario, data: scenarios[r.scenario] });
    emitRoom(room, "roomUpdate", roomSnapshot(r));
  });

  socket.on("submitAnswer", ({ room, choice, answer, reflection }) => {
    room = String(room || "").toUpperCase();
    const r = rooms.get(room);
    const student = r?.students.get(socket.id);
    if (!r || !student || !r.started) return socket.emit("serverError", "ยังไม่เปิดรับคำตอบ");

    const item = {
      id: socket.id,
      name: student.name,
      choice: Number(choice),
      answer: String(answer || "").slice(0, 1500),
      reflection: String(reflection || "").slice(0, 1000),
      scenario: r.scenario,
      at: new Date().toISOString()
    };
    r.responses.set(socket.id, item);

    io.to(r.host).emit("newResponse", item);
    emitRoom(room, "roomUpdate", roomSnapshot(r));
    socket.emit("answerSaved", { ok: true });
  });

  socket.on("disconnect", () => {
    const room = socket.data.room;
    if (!room) return;
    const r = rooms.get(room);
    if (!r) return;

    if (r.host === socket.id) {
      rooms.delete(room);
      emitRoom(room, "roomClosed", {});
    } else {
      r.students.delete(socket.id);
      r.responses.delete(socket.id);
      emitRoom(room, "roomUpdate", roomSnapshot(r));
    }
  });
});

app.get("/health", (req, res) => {
  res.json({ ok: true, service: "decision-lab-domino", rooms: rooms.size });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`Decision Lab running on port ${PORT}`);
});
