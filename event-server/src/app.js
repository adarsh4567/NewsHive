const express = require('express');
const cors = require('cors');
const newsRoutes = require('./routes/news.routes');

const app = express();

app.use(express.json());
app.use(cors());

// Mount routes
app.use('/', newsRoutes);

module.exports = app;
