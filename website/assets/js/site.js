const fadeTargets = document.querySelectorAll('.fade');
    if ('IntersectionObserver' in window) {
      const revealObserver = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add('show');
            revealObserver.unobserve(entry.target);
          }
        });
      }, { threshold: 0.12 });

      fadeTargets.forEach((el) => revealObserver.observe(el));
    } else {
      fadeTargets.forEach((el) => el.classList.add('show'));
    }

    const topbar = document.querySelector('.topbar');
    const menuToggle = document.querySelector('.menu-toggle');
    const mobileMenu = document.querySelector('#mobile-menu');
    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    const getAnchorOffset = () => {
      return (topbar ? topbar.offsetHeight : 0) + 18;
    };

    const scrollToTarget = (target, behavior = 'smooth') => {
      if (!target) {
        return;
      }

      const top = Math.max(0, target.getBoundingClientRect().top + window.scrollY - getAnchorOffset());
      window.scrollTo({
        top,
        behavior: prefersReducedMotion ? 'auto' : behavior,
      });
    };

    const closeMobileMenu = () => {
      topbar.classList.remove('menu-open');
      menuToggle.setAttribute('aria-expanded', 'false');
      mobileMenu.hidden = true;
    };

    menuToggle.addEventListener('click', () => {
      const isOpen = menuToggle.getAttribute('aria-expanded') === 'true';
      menuToggle.setAttribute('aria-expanded', String(!isOpen));
      mobileMenu.hidden = isOpen;
      topbar.classList.toggle('menu-open', !isOpen);
    });

    mobileMenu.querySelectorAll('a').forEach((link) => {
      link.addEventListener('click', () => closeMobileMenu());
    });

    document.querySelectorAll('a[href^="#"]').forEach((link) => {
      link.addEventListener('click', (event) => {
        const hash = link.getAttribute('href');
        if (!hash || hash === '#') {
          return;
        }

        const target = document.querySelector(hash);
        if (!target) {
          return;
        }

        event.preventDefault();
        scrollToTarget(target);
      });
    });

    window.addEventListener('resize', () => {
      if (window.innerWidth > 720 && !mobileMenu.hidden) {
        closeMobileMenu();
      }
    });
