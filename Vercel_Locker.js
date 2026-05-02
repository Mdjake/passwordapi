export default async function handler(req, res) {
  // Only allow POST requests
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { password } = req.body;

  if (!password || typeof password !== 'string') {
    return res.status(400).json({ error: 'Password is required' });
  }

  // Get password from Vercel environment variable
  const correctPassword = process.env.SITE_PASSWORD;

  if (!correctPassword) {
    console.error('SITE_PASSWORD not set');
    return res.status(500).json({ error: 'Server configuration error' });
  }

  if (password === correctPassword) {
    return res.status(200).json({ success: true, message: 'Access granted' });
  } else {
    return res.status(401).json({ success: false, message: 'Invalid password' });
  }
}
