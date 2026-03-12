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
    const pageBody = document.body;

    pageBody.classList.add('page-enhanced');
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        pageBody.classList.add('page-ready');
      });
    });

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

    window.addEventListener('pageshow', () => {
      pageBody.classList.remove('page-leaving');
      pageBody.classList.add('page-ready');
    });

    document.querySelectorAll('a[data-page-transition]').forEach((link) => {
      link.addEventListener('click', (event) => {
        if (event.defaultPrevented || event.button !== 0) {
          return;
        }

        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
          return;
        }

        if (link.target === '_blank' || link.hasAttribute('download')) {
          return;
        }

        const url = new URL(link.href, window.location.href);
        if (url.origin !== window.location.origin) {
          return;
        }

        event.preventDefault();
        pageBody.classList.remove('page-ready');
        pageBody.classList.add('page-leaving');

        if (!mobileMenu.hidden) {
          closeMobileMenu();
        }

        window.setTimeout(() => {
          window.location.href = url.href;
        }, prefersReducedMotion ? 0 : 280);
      });
    });

    const planPriceNodes = Array.from(document.querySelectorAll('.plans .plan .price'));
    if (planPriceNodes.length) {
      const defaultPlans = Object.freeze({
        pro: 99,
        proPlus: 199,
        proUltra: 349,
      });

      const planKeysByIndex = ['pro', 'proPlus', 'proUltra'];
      const premiumSection = document.querySelector('#premium .container');

      let pricingContext = document.querySelector('[data-pricing-context]');
      if (!pricingContext && premiumSection) {
        const pricingMeta = document.createElement('p');
        pricingMeta.className = 'pricing-meta';
        pricingMeta.innerHTML = '<span data-pricing-context>Prices shown in INR for India. Regional currency estimates load automatically.</span> <a class="pricing-source" href="https://www.exchangerate-api.com" target="_blank" rel="noreferrer">Rates by ExchangeRate-API</a>';
        premiumSection.appendChild(pricingMeta);
        pricingContext = pricingMeta.querySelector('[data-pricing-context]');
      }

      const ensurePriceMarkup = () => {
        planPriceNodes.forEach((node, index) => {
          node.dataset.planKey = planKeysByIndex[index] || '';

          if (node.querySelector('.price-amount')) {
            return;
          }

          const amountNode = document.createElement('span');
          amountNode.className = 'price-amount';
          amountNode.textContent = node.childNodes[0] ? node.childNodes[0].textContent.trim() : '';

          const perNode = node.querySelector('.per');
          node.textContent = '';
          node.appendChild(amountNode);

          if (perNode) {
            node.appendChild(document.createTextNode(' '));
            node.appendChild(perNode);
          }
        });
      };

      const formatPrice = (amount, currency) => {
        const locale = navigator.language || 'en-US';

        try {
          return new Intl.NumberFormat(locale, {
            style: 'currency',
            currency,
            currencyDisplay: 'narrowSymbol',
          }).format(amount);
        } catch (error) {
          return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency,
            currencyDisplay: 'symbol',
          }).format(amount);
        }
      };

      const renderPlanPrices = (currency, plans) => {
        ensurePriceMarkup();

        planPriceNodes.forEach((node) => {
          const planKey = node.dataset.planKey;
          const amountNode = node.querySelector('.price-amount');
          const amount = plans[planKey];

          if (!amountNode || typeof amount !== 'number') {
            return;
          }

          amountNode.textContent = formatPrice(amount, currency);
        });
      };

      const updatePricingContext = (currency, fallback) => {
        if (!pricingContext) {
          return;
        }

        if (currency === 'INR') {
          pricingContext.textContent = 'Prices shown in INR for India.';
          return;
        }

        if (fallback && currency === 'USD') {
          pricingContext.textContent = 'Showing USD estimates for your region. Final charge may vary slightly with exchange rates.';
          return;
        }

        pricingContext.textContent = `Showing estimated prices in ${currency} for your region. Final charge may vary slightly with exchange rates.`;
      };

      renderPlanPrices('INR', defaultPlans);
      updatePricingContext('INR', false);

      fetch('/api/pricing', {
        headers: {
          accept: 'application/json',
        },
      })
        .then((response) => {
          if (!response.ok) {
            throw new Error(`Pricing request failed with ${response.status}`);
          }

          return response.json();
        })
        .then((payload) => {
          if (!payload || !payload.plans || !payload.displayCurrency) {
            return;
          }

          renderPlanPrices(payload.displayCurrency, payload.plans);
          updatePricingContext(payload.displayCurrency, Boolean(payload.fallback));
        })
        .catch(() => {
          renderPlanPrices('INR', defaultPlans);
          updatePricingContext('INR', true);
        });
    }
