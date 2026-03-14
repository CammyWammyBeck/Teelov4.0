/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/teelo/web/templates/**/*.html",
    "./src/teelo/web/static/js/**/*.js"
  ],
  theme: {
    extend: {
      colors: {
        teelo: {
          lime: 'var(--color-teelo-lime)',
          blue: 'var(--color-teelo-blue)',
          ink: 'var(--color-teelo-ink)',
          light: 'var(--color-teelo-light)',
          dark: 'var(--color-teelo-dark)',
          surface: 'var(--surface)',
        },
        // Semantic surface/background tokens
        surface: {
          DEFAULT: 'var(--surface)',
          alt: 'var(--surface-alt)',
          muted: 'var(--surface-muted)',
          hover: 'var(--surface-hover)',
          inverse: 'var(--surface-inverse)',
        },
        // Semantic text/content tokens
        content: {
          DEFAULT: 'var(--content)',
          secondary: 'var(--content-secondary)',
          muted: 'var(--content-muted)',
          faint: 'var(--content-faint)',
          faintest: 'var(--content-faintest)',
          inverse: 'var(--content-inverse)',
        },
        // Semantic border tokens
        line: {
          DEFAULT: 'var(--line)',
          strong: 'var(--line-strong)',
          subtle: 'var(--line-subtle)',
        },
        // Status tokens
        status: {
          success: 'var(--status-success)',
          danger: 'var(--status-danger)',
          'success-bg': 'var(--status-success-bg)',
          'danger-bg': 'var(--status-danger-bg)',
        },
        // Tour brand colors (constant across themes)
        tour: {
          atp: '#002865',
          wta: '#E30066',
          challenger: '#006B3F',
          itf: '#6B7280',
          grandslam: '#D4AF37',
          '250': '#9D174D',
        },
        // Gender colors
        gender: {
          male: '#3B82F6',
          female: '#EC4899',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      boxShadow: {
        'soft': '0 4px 20px -2px var(--shadow-color, rgba(0, 0, 0, 0.05))',
        'glow': '0 0 15px var(--glow-color, rgba(204, 255, 0, 0.4))',
      },
      typography: (theme) => ({
        DEFAULT: {
          css: {
            color: 'var(--content-secondary)',
            maxWidth: 'none',
            '--tw-prose-headings': theme('colors.teelo.dark'),
            '--tw-prose-links': theme('colors.teelo.dark'),
            h1: {
              fontWeight: '800',
            },
            h2: {
              fontWeight: '700',
              marginTop: '1.5em',
            },
            h3: {
              fontWeight: '600',
            },
            strong: {
              color: 'var(--content)',
              fontWeight: '700',
            },
            a: {
              fontWeight: '600',
              textDecoration: 'underline',
              textDecorationColor: theme('colors.teelo.lime'),
              textDecorationThickness: '2px',
              '&:hover': {
                backgroundColor: theme('colors.teelo.lime'),
                color: theme('colors.teelo.dark'),
                textDecoration: 'none',
              },
            },
            code: {
              color: 'var(--content-inverse)',
              backgroundColor: theme('colors.teelo.lime'),
              padding: '0.2em 0.4em',
              borderRadius: '0.25rem',
              fontWeight: '500',
            },
            'code::before': { content: '""' },
            'code::after': { content: '""' },
            pre: {
              backgroundColor: 'var(--surface-inverse)',
              color: 'var(--content-inverse)',
            },
            table: {
              width: '100%',
              marginTop: '1em',
              marginBottom: '1em',
            },
            th: {
              color: 'var(--content)',
              fontWeight: '600',
              textAlign: 'left',
            },
          },
        },
      }),
    }
  },
  plugins: [
    require('@tailwindcss/typography'),
  ],
}
