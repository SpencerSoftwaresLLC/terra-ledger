const { app, BrowserWindow, dialog, shell } = require("electron");
const path = require("path");
const { spawn } = require("child_process");
const http = require("http");
const fs = require("fs");
const { autoUpdater } = require("electron-updater");

let mainWindow;
let flaskProcess;

function waitForServer(url, timeout = 25000, interval = 500) {
  return new Promise((resolve, reject) => {
    const start = Date.now();

    function check() {
      http
        .get(url, (res) => {
          res.resume();
          resolve(true);
        })
        .on("error", () => {
          if (Date.now() - start > timeout) {
            reject(new Error(`Server did not start in time: ${url}`));
          } else {
            setTimeout(check, interval);
          }
        });
    }

    check();
  });
}

function startFlask() {
  let projectRoot;
  let flaskAppPath;
  let pythonCmd;

  if (app.isPackaged) {
    projectRoot = process.resourcesPath;
    flaskAppPath = path.join(projectRoot, "app.py");
    pythonCmd = path.join(projectRoot, "venv", "Scripts", "python.exe");
  } else {
    projectRoot = path.join(__dirname, "..");
    flaskAppPath = path.join(projectRoot, "app.py");
    pythonCmd =
      process.platform === "win32"
        ? path.join(projectRoot, "venv", "Scripts", "python.exe")
        : path.join(projectRoot, "venv", "bin", "python");
  }

  if (!fs.existsSync(pythonCmd)) {
    pythonCmd = "python";
  }

  flaskProcess = spawn(pythonCmd, [flaskAppPath], {
    cwd: projectRoot,
    shell: true,
    env: { ...process.env }
  });

  flaskProcess.stdout.on("data", (data) => {
    console.log(`[Flask] ${data}`);
  });

  flaskProcess.stderr.on("data", (data) => {
    console.error(`[Flask Error] ${data}`);
  });

  flaskProcess.on("close", (code) => {
    console.log(`Flask process exited with code ${code}`);
  });

  flaskProcess.on("error", (err) => {
    console.error("Failed to start Flask process:", err);
  });
}

async function createWindow() {
  const iconPath = app.isPackaged
    ? path.join(process.resourcesPath, "build", "icon.ico")
    : path.join(__dirname, "build", "icon.ico");

  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    icon: iconPath,
    webPreferences: {
      devTools: true
    },
    show: false
  });

  // Open target="_blank" links in the user's real browser
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });

  // Keep TerraLedger local routes inside the app, send all outside URLs to browser
  mainWindow.webContents.on("will-navigate", (event, url) => {
    const isLocal =
      url.startsWith("http://127.0.0.1:5000") ||
      url.startsWith("http://localhost:5000");

    if (!isLocal) {
      event.preventDefault();
      shell.openExternal(url);
    }
  });

  try {
    await waitForServer("http://127.0.0.1:5000");
    await mainWindow.loadURL("http://127.0.0.1:5000");
    mainWindow.show();
  } catch (error) {
    console.error(error);

    await mainWindow.loadURL(
      "data:text/html;charset=utf-8," +
        encodeURIComponent(`
          <html>
            <body style="font-family: Arial, sans-serif; padding: 40px; background: #f7f7f5; color: #1f2933;">
              <h2>TerraLedger could not start</h2>
              <p>The desktop app opened, but the Flask server did not start correctly.</p>
              <pre>${error.message}</pre>
            </body>
          </html>
        `)
    );

    mainWindow.show();
    mainWindow.webContents.openDevTools();
  }
}

function setupAutoUpdates() {
  autoUpdater.autoDownload = true;
  autoUpdater.autoInstallOnAppQuit = true;

  autoUpdater.on("checking-for-update", () => {
    console.log("Checking for update...");
  });

  autoUpdater.on("update-available", (info) => {
    console.log("Update available:", info.version);
  });

  autoUpdater.on("update-not-available", () => {
    console.log("No update available.");
  });

  autoUpdater.on("error", (err) => {
    console.error("Auto update error:", err);
  });

  autoUpdater.on("download-progress", (progress) => {
    console.log(`Download speed: ${progress.bytesPerSecond}`);
    console.log(`Downloaded ${progress.percent}%`);
  });

  autoUpdater.on("update-downloaded", async (info) => {
    const result = await dialog.showMessageBox({
      type: "info",
      buttons: ["Restart Now", "Later"],
      defaultId: 0,
      cancelId: 1,
      title: "Update Ready",
      message: `TerraLedger ${info.version} has been downloaded.`,
      detail: "Restart the app to install the update."
    });

    if (result.response === 0) {
      autoUpdater.quitAndInstall();
    }
  });

  setTimeout(() => {
    autoUpdater.checkForUpdatesAndNotify();
  }, 5000);
}

app.whenReady().then(async () => {
  startFlask();
  await createWindow();

  if (app.isPackaged) {
    setupAutoUpdates();
  }

  app.on("activate", async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (flaskProcess) {
    flaskProcess.kill();
  }

  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", () => {
  if (flaskProcess) {
    flaskProcess.kill();
  }
});