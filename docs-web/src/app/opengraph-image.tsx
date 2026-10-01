import { ImageResponse } from 'next/og';

export const revalidate = false;
export const size = { width: 1200, height: 630 };
export const contentType = 'image/png';

const BG = '#07080a';
const BORDER = 'rgba(255,255,255,0.09)';
const ACCENT = '#a78bfa';
const ACCENT_DIM = 'rgba(167,139,250,0.35)';

export default function Image() {
  return new ImageResponse(
    (
      <div
        style={{
          width: '100%',
          height: '100%',
          display: 'flex',
          position: 'relative',
          backgroundColor: BG,
          fontFamily: 'sans-serif',
          overflow: 'hidden',
        }}
      >
        {/* dot-grid texture */}
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            backgroundImage: 'radial-gradient(rgba(255,255,255,0.07) 1.5px, transparent 1.5px)',
            backgroundSize: '28px 28px',
          }}
        />
        {/* glow */}
        <div
          style={{
            position: 'absolute',
            top: '-260px',
            left: '-200px',
            width: '760px',
            height: '760px',
            display: 'flex',
            borderRadius: '760px',
            backgroundImage: `radial-gradient(circle, ${ACCENT_DIM} 0%, rgba(167,139,250,0) 65%)`,
          }}
        />

        {/* content */}
        <div
          style={{
            position: 'relative',
            display: 'flex',
            width: '100%',
            height: '100%',
            padding: '64px',
            justifyContent: 'space-between',
          }}
        >
          {/* left column */}
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'space-between',
              width: '610px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <div
                style={{
                  width: '9px',
                  height: '9px',
                  borderRadius: '9px',
                  backgroundColor: ACCENT,
                  display: 'flex',
                }}
              />
              <span
                style={{
                  fontSize: '20px',
                  fontWeight: 600,
                  color: '#9a9aa5',
                  letterSpacing: '0.16em',
                  textTransform: 'uppercase',
                }}
              >
                Async Task Queue
              </span>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '22px' }}>
              <span
                style={{
                  fontSize: '104px',
                  fontWeight: 800,
                  color: '#ffffff',
                  letterSpacing: '-0.03em',
                  lineHeight: 1,
                }}
              >
                BlitzQ
              </span>
              <span
                style={{
                  fontSize: '28px',
                  fontWeight: 500,
                  color: '#b7b7c2',
                  lineHeight: 1.45,
                  width: '560px',
                }}
              >
                Async-native task queue for Python, built on Redis. Reliable
                and fast delivery modes, benchmarked against Celery.
              </span>
            </div>

            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '12px',
                padding: '14px 22px',
                width: 'fit-content',
                borderRadius: '14px',
                border: `1px solid ${ACCENT_DIM}`,
                backgroundColor: 'rgba(167,139,250,0.08)',
              }}
            >
              <span
                style={{
                  display: 'flex',
                  fontSize: '34px',
                  fontWeight: 800,
                  color: ACCENT,
                  letterSpacing: '-0.01em',
                }}
              >
                18.9x
              </span>
              <span
                style={{
                  display: 'flex',
                  fontSize: '19px',
                  fontWeight: 600,
                  color: '#d4d4dc',
                }}
              >
                faster than other task queues, tuned workloads
              </span>
            </div>

            <div style={{ display: 'flex', gap: '12px' }}>
              {['REDIS', 'ASYNCIO', 'PYTHON'].map((tag) => (
                <div
                  key={tag}
                  style={{
                    display: 'flex',
                    padding: '10px 18px',
                    borderRadius: '999px',
                    border: `1px solid ${BORDER}`,
                    fontSize: '20px',
                    fontWeight: 600,
                    color: '#d4d4dc',
                    letterSpacing: '0.04em',
                  }}
                >
                  {tag}
                </div>
              ))}
            </div>
          </div>

          {/* right column: terminal card */}
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              width: '430px',
              height: '100%',
              justifyContent: 'center',
            }}
          >
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                borderRadius: '18px',
                border: `1px solid ${BORDER}`,
                backgroundColor: 'rgba(255,255,255,0.03)',
                boxShadow: '0 40px 80px rgba(0,0,0,0.55)',
                overflow: 'hidden',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  padding: '18px 20px',
                  borderBottom: `1px solid ${BORDER}`,
                }}
              >
                {['#ff5f56', '#ffbd2e', '#27c93f'].map((c) => (
                  <div
                    key={c}
                    style={{
                      width: '11px',
                      height: '11px',
                      borderRadius: '11px',
                      backgroundColor: c,
                      display: 'flex',
                    }}
                  />
                ))}
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '14px',
                  padding: '26px 24px 30px',
                  fontFamily: 'monospace',
                  fontSize: '19px',
                  lineHeight: 1.6,
                }}
              >
                <span style={{ display: 'flex', color: '#6ee7b7' }}>
                  $ pip install blitzq
                </span>
                <span style={{ display: 'flex', color: '#6b7280' }}>
                  {'# app.py'}
                </span>
                <span style={{ display: 'flex' }}>
                  <span style={{ color: '#c084fc' }}>@queue.task</span>
                  <span style={{ color: '#9ca3af' }}>(retries=3)</span>
                </span>
                <span style={{ display: 'flex' }}>
                  <span style={{ color: '#60a5fa' }}>async def </span>
                  <span style={{ color: '#fbbf24' }}>process_order</span>
                  <span style={{ color: '#e5e7eb' }}>(order_id):</span>
                </span>
                <span style={{ display: 'flex', paddingLeft: '28px', color: '#60a5fa' }}>
                  return <span style={{ color: '#e5e7eb' }}>{'{"ok": True}'}</span>
                </span>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                marginTop: '20px',
                alignSelf: 'flex-end',
                padding: '10px 20px',
                borderRadius: '999px',
                border: `1px solid ${ACCENT_DIM}`,
                color: ACCENT,
                fontSize: '20px',
                fontWeight: 700,
                letterSpacing: '0.02em',
              }}
            >
              v1.1.0
            </div>
          </div>
        </div>
      </div>
    ),
    { ...size }
  );
}
