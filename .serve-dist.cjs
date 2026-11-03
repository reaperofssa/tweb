const express = require('express');
const compression = require('compression');
const path = require('path');
const dist = path.join(__dirname, 'dist');
const app = express();
app.use(compression());
app.use(express.static(dist, {
  index: false,
  setHeaders(res, file) {
    if (/(index\.html|sw[^/]*\.js|manifest.*\.json)$/.test(file)) res.setHeader('Cache-Control', 'no-cache');
    else if (/-[\w-]{8}\.\w+$/.test(file)) res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  }
}));
app.use((req, res) => res.set('Cache-Control', 'no-cache').sendFile(path.join(dist, 'index.html')));
app.listen(8080, '0.0.0.0', () => console.log('frontend on http://localhost:8080'));
