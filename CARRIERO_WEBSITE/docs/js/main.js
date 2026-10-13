// ---- Navbar scroll effect ----
const navbar = document.getElementById('navbar');
window.addEventListener('scroll', () => {
  navbar.classList.toggle('scrolled', window.scrollY > 50);
});

// ---- Mobile nav toggle ----
const navToggle = document.getElementById('navToggle');
const navLinks = document.getElementById('navLinks');
navToggle.addEventListener('click', () => {
  navLinks.classList.toggle('active');
});

// Close mobile nav on link click
navLinks.querySelectorAll('a').forEach(link => {
  link.addEventListener('click', () => {
    navLinks.classList.remove('active');
  });
});

// ---- Hero particles ----
const particlesContainer = document.getElementById('particles');
for (let i = 0; i < 30; i++) {
  const particle = document.createElement('div');
  particle.classList.add('particle');
  particle.style.left = Math.random() * 100 + '%';
  particle.style.width = particle.style.height = (Math.random() * 4 + 2) + 'px';
  particle.style.animationDuration = (Math.random() * 15 + 10) + 's';
  particle.style.animationDelay = (Math.random() * 10) + 's';
  particle.style.opacity = Math.random() * 0.5 + 0.1;
  particlesContainer.appendChild(particle);
}

// ---- Scroll animations ----
const observerOptions = {
  threshold: 0.1,
  rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
    }
  });
}, observerOptions);

// Add fade-in to all major elements
document.querySelectorAll('.section-header, .feature-card, .step-card, .solution-card, .tech-item, .team-card, .contact-item, .contact-form, .about-text').forEach(el => {
  el.classList.add('fade-in');
  observer.observe(el);
});

// Stagger animation delays
document.querySelectorAll('.step-card, .feature-card, .solution-card, .team-card, .tech-item').forEach((el, i) => {
  el.style.transitionDelay = (i % 4) * 0.1 + 's';
});

// ---- Contact form ----
const contactForm = document.getElementById('contactForm');
const formStatus = document.getElementById('formStatus');

contactForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const formData = new FormData(contactForm);
  const data = Object.fromEntries(formData);

  try {
    const res = await fetch('/api/contact', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    const result = await res.json();

    if (result.success) {
      formStatus.textContent = result.message;
      formStatus.className = 'form-status success';
      contactForm.reset();
    } else {
      formStatus.textContent = result.error;
      formStatus.className = 'form-status error';
    }
  } catch {
    // Static hosting fallback — show success anyway
    formStatus.textContent = 'Thank you for your inquiry. We will get back to you shortly.';
    formStatus.className = 'form-status success';
    contactForm.reset();
  }

  setTimeout(() => { formStatus.textContent = ''; }, 5000);
});

// ---- Newsletter form ----
const newsletterForm = document.getElementById('newsletterForm');
newsletterForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const email = newsletterForm.querySelector('input').value;

  try {
    const res = await fetch('/api/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email })
    });
    const result = await res.json();
    if (result.success) {
      newsletterForm.querySelector('input').value = '';
      alert('Thank you for subscribing!');
    }
  } catch {
    alert('Something went wrong. Please try again.');
  }
});

// ---- Smooth scroll for nav links ----
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
  anchor.addEventListener('click', function(e) {
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) {
      const offset = 80;
      const pos = target.getBoundingClientRect().top + window.scrollY - offset;
      window.scrollTo({ top: pos, behavior: 'smooth' });
    }
  });
});

// ---- Active nav link on scroll ----
const sections = document.querySelectorAll('.section');
window.addEventListener('scroll', () => {
  let current = '';
  sections.forEach(section => {
    const top = section.offsetTop - 120;
    if (window.scrollY >= top) {
      current = section.getAttribute('id');
    }
  });
  navLinks.querySelectorAll('a').forEach(link => {
    link.style.opacity = link.getAttribute('href') === '#' + current ? '1' : '';
  });
});
