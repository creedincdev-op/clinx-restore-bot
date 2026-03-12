(function () {
  const body = document.body;
  const topbar = document.querySelector('.topbar');
  const menuToggle = document.querySelector('.menu-toggle');
  const mobileMenu = document.querySelector('#mobile-menu');
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const homeSectionKey = 'clinx-home-section';

  const normalizePath = (path) => {
    const trimmed = path.replace(/\/index(?:\.html)?$/, '/');
    return trimmed === '' ? '/' : trimmed;
  };

  const isHomePage = () => normalizePath(window.location.pathname) === '/';

  const getAnchorOffset = () => {
    return (topbar ? topbar.offsetHeight : 0) + 18;
  };

  const scrollToTarget = (target, behavior) => {
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
    if (!topbar || !menuToggle || !mobileMenu) {
      return;
    }

    topbar.classList.remove('menu-open');
    menuToggle.setAttribute('aria-expanded', 'false');
    mobileMenu.hidden = true;
  };

  const transitionTo = (href) => {
    const destination = href || '/';
    if (prefersReducedMotion || body.classList.contains('page-leaving')) {
      window.location.href = destination;
      return;
    }

    body.classList.add('page-leaving');
    window.setTimeout(() => {
      window.location.href = destination;
    }, 260);
  };

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

  body.classList.add('page-enhanced');
  window.requestAnimationFrame(() => {
    body.classList.add('page-ready');
  });

  window.addEventListener('pageshow', () => {
    body.classList.remove('page-leaving');
    body.classList.add('page-ready');
  });

  if (menuToggle && mobileMenu) {
    menuToggle.addEventListener('click', () => {
      const isOpen = menuToggle.getAttribute('aria-expanded') === 'true';
      menuToggle.setAttribute('aria-expanded', String(!isOpen));
      mobileMenu.hidden = isOpen;
      if (topbar) {
        topbar.classList.toggle('menu-open', !isOpen);
      }
    });

    mobileMenu.querySelectorAll('a').forEach((link) => {
      link.addEventListener('click', () => closeMobileMenu());
    });
  }

  document.querySelectorAll('[data-section-link]').forEach((link) => {
    link.addEventListener('click', (event) => {
      const targetId = link.getAttribute('data-section-link');
      if (!targetId) {
        return;
      }

      if (isHomePage()) {
        const target = document.getElementById(targetId);
        if (!target) {
          return;
        }

        event.preventDefault();
        closeMobileMenu();
        scrollToTarget(target, 'smooth');
        return;
      }

      try {
        window.sessionStorage.setItem(homeSectionKey, targetId);
      } catch (error) {
        // Ignore storage failures and continue home.
      }

      event.preventDefault();
      closeMobileMenu();
      transitionTo('/');
    });
  });

  if (isHomePage()) {
    let pendingSection;

    try {
      pendingSection = window.sessionStorage.getItem(homeSectionKey);
      window.sessionStorage.removeItem(homeSectionKey);
    } catch (error) {
      pendingSection = null;
    }

    if (pendingSection) {
      window.requestAnimationFrame(() => {
        window.setTimeout(() => {
          scrollToTarget(document.getElementById(pendingSection), 'smooth');
        }, 120);
      });
    }
  }

  document.querySelectorAll('a[data-page-transition="true"]').forEach((link) => {
    link.addEventListener('click', (event) => {
      if (event.defaultPrevented || link.hasAttribute('download')) {
        return;
      }

      if (link.dataset.sectionLink) {
        return;
      }

      const href = link.getAttribute('href');
      if (!href || href.startsWith('#') || (link.target && link.target !== '_self')) {
        return;
      }

      const url = new URL(href, window.location.href);
      if (url.origin !== window.location.origin) {
        return;
      }

      const next = url.pathname + url.search + url.hash;
      const current = window.location.pathname + window.location.search + window.location.hash;
      if (next === current) {
        return;
      }

      event.preventDefault();
      closeMobileMenu();
      transitionTo(next);
    });
  });

  window.addEventListener('resize', () => {
    if (window.innerWidth > 760 && mobileMenu && !mobileMenu.hidden) {
      closeMobileMenu();
    }
  });

  document.querySelectorAll('[data-tablist]').forEach((tablist) => {
    const tabs = Array.from(tablist.querySelectorAll('[role="tab"]'));
    if (!tabs.length) {
      return;
    }

    const activateTab = (tabToActivate, moveFocus) => {
      tabs.forEach((tab) => {
        const active = tab === tabToActivate;
        tab.setAttribute('aria-selected', String(active));
        tab.tabIndex = active ? 0 : -1;

        const panelId = tab.getAttribute('aria-controls');
        const panel = panelId ? document.getElementById(panelId) : null;
        if (panel) {
          panel.hidden = !active;
        }
      });

      if (moveFocus) {
        tabToActivate.focus();
      }
    };

    tabs.forEach((tab, index) => {
      tab.addEventListener('click', () => activateTab(tab, false));
      tab.addEventListener('keydown', (event) => {
        let nextIndex = index;

        if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
          nextIndex = (index + 1) % tabs.length;
        } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
          nextIndex = (index - 1 + tabs.length) % tabs.length;
        } else if (event.key === 'Home') {
          nextIndex = 0;
        } else if (event.key === 'End') {
          nextIndex = tabs.length - 1;
        } else {
          return;
        }

        event.preventDefault();
        activateTab(tabs[nextIndex], true);
      });
    });

    const defaultTab = tabs.find((tab) => tab.getAttribute('aria-selected') === 'true') || tabs[0];
    activateTab(defaultTab, false);
  });

  const priceBlocks = Array.from(document.querySelectorAll('[data-plan-price]'));
  const pricingNote = document.querySelector('[data-pricing-note]');

  const renderFallbackPricing = () => {
    priceBlocks.forEach((block) => {
      const value = Number(block.getAttribute('data-price-inr'));
      const target = block.querySelector('[data-price-value]');
      if (target) {
        target.textContent = `INR ${value.toFixed(2)}`;
      }
    });

    if (pricingNote) {
      pricingNote.textContent = 'Prices stay in INR by default and switch to local estimates when available.';
    }
  };

  const renderPricing = (payload) => {
    if (!payload || !payload.currency || !payload.prices) {
      renderFallbackPricing();
      return;
    }

    const locale = payload.locale || undefined;
    const formatter = new Intl.NumberFormat(locale, {
      style: 'currency',
      currency: payload.currency,
    });

    priceBlocks.forEach((block) => {
      const planKey = block.getAttribute('data-plan-price');
      const amount = payload.prices[planKey];
      const target = block.querySelector('[data-price-value]');
      if (typeof amount !== 'number' || !target) {
        return;
      }

      target.textContent = formatter.format(amount);
    });

    if (pricingNote) {
      pricingNote.textContent = payload.message || 'Prices shown in your local currency.';
    }
  };

  if (priceBlocks.length) {
    window.fetch('/api/pricing')
      .then((response) => {
        if (!response.ok) {
          throw new Error('pricing unavailable');
        }

        return response.json();
      })
      .then((payload) => {
        renderPricing(payload);
      })
      .catch(() => {
        renderFallbackPricing();
      });
  }
}());
