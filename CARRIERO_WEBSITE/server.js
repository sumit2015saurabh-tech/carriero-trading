const express = require('express');
const path = require('path');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// API: Contact form submission
app.post('/api/contact', (req, res) => {
  const { name, email, phone, message } = req.body;

  if (!name || !email || !message) {
    return res.status(400).json({ success: false, error: 'Name, email, and message are required.' });
  }

  // Log the inquiry (in production, store in DB or send email)
  console.log('--- New Contact Inquiry ---');
  console.log(`Name: ${name}`);
  console.log(`Email: ${email}`);
  console.log(`Phone: ${phone || 'N/A'}`);
  console.log(`Message: ${message}`);
  console.log('---------------------------');

  res.json({ success: true, message: 'Thank you for your inquiry. We will get back to you shortly.' });
});

// API: Newsletter subscription
app.post('/api/subscribe', (req, res) => {
  const { email } = req.body;

  if (!email) {
    return res.status(400).json({ success: false, error: 'Email is required.' });
  }

  console.log(`New subscriber: ${email}`);
  res.json({ success: true, message: 'Thank you for subscribing!' });
});

// Serve index.html for all other routes (SPA fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Carriero server running on port ${PORT}`);
  console.log(`Visit: http://localhost:${PORT}`);
});
