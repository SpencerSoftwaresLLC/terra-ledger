const { app, BrowserWindow } = require('electron');

function createWindow() {
  const win = new BrowserWindow({
    width: 1300,
    height: 850,
    autoHideMenuBar: true,
  });

  win.loadURL('https://www.terraledger.net');
}

app.whenReady().then(createWindow);