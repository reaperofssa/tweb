import {createSignal, onCleanup, onMount, Show} from 'solid-js';

import Button from '@components/buttonTsx';
import MediaHeader from '@components/mediaHeader';
import {toastNew} from '@components/toast';

import AuthCard from '@/pages/AuthCard';
import {CardSpec, useAuthFlow} from '@/pages/authFlow';
import {sentCodeToCardSpec, withPhoneNumber} from '@/pages/sentCode';
import styles from '@/pages/authFlow.module.scss';

if(import.meta.hot) import.meta.hot.accept();

type Spec = Extract<CardSpec, {name: 'waPairing'}>;

const POLL_INTERVAL = 2000;

/**
 * WhatsApp login. Unlike Telegram's code card nothing is typed here: the server asked WhatsApp for a
 * pairing code and sent it back inside the `auth.sentCode` (`type.pairing_code`). The user types it in
 * WhatsApp > Linked devices > Link with phone number, and meanwhile this card polls `auth.signIn`:
 *   WA_PAIRING_PENDING  -> keep waiting
 *   auth.authorization  -> setUser + toIm (same as the code card)
 *   PHONE_CODE_EXPIRED  -> offer a new code via `auth.resendCode`
 */
export default function WaPairingCard(props: {spec: Spec}) {
  const {managers, navigate, toIm, isLeaving} = useAuthFlow();

  const sentCode = props.spec.payload;
  const type = sentCode.type as any as {pairing_code: string, expires_in?: number};
  const phone_number = sentCode.phone_number;
  const phone_code_hash = sentCode.phone_code_hash;
  const code = type.pairing_code;
  // the countdown runs on this tab's clock from the moment the code is shown: no server clock skew to deal with
  const deadline = Date.now() + (type.expires_in || 120) * 1000;

  const [left, setLeft] = createSignal(Math.max(0, Math.round((deadline - Date.now()) / 1000)));
  const [expired, setExpired] = createSignal(false);
  const [busy, setBusy] = createSignal(false);

  let stopped = false;
  let pollTimer: number | undefined;
  let tickTimer: number | undefined;

  const stop = () => {
    stopped = true;
    clearTimeout(pollTimer);
    clearInterval(tickTimer);
  };

  function poll() {
    if(stopped || isLeaving()) return;
    managers.appAccountManager.signIn({phone_number, phone_code_hash} as any).then(async(response) => {
      if(stopped) return;
      if(response._ === 'auth.authorization') {
        stop();
        await managers.apiManager.setUser(response.user);
        toIm();
        return;
      }
      pollTimer = window.setTimeout(poll, POLL_INTERVAL);
    }).catch((err: ApiError) => {
      if(stopped) return;
      // WA_PAIRING_PENDING / NETWORK_ERROR are not in tweb's ErrorType union, hence the string
      switch(err.type as string) {
        case 'WA_PAIRING_PENDING':
          pollTimer = window.setTimeout(poll, POLL_INTERVAL);
          break;
        case 'PHONE_CODE_EXPIRED':
        case 'PHONE_CODE_INVALID':
          stop();
          setExpired(true);
          break;
        case 'NETWORK_ERROR':
          pollTimer = window.setTimeout(poll, POLL_INTERVAL * 2); // the backend is restarting: keep trying
          break;
        default:
          console.error('waPairing: auth.signIn error', err);
          stop();
          toastNew({langPackKey: 'Error.AnError'});
          navigate({name: 'signIn'});
          break;
      }
    });
  }

  function newCode() {
    if(busy()) return;
    setBusy(true);
    managers.appAccountManager.resendCode(phone_number, phone_code_hash).then((answer) => {
      stop();
      if(answer._ === 'auth.sentCodeSuccess') { // the number was linked in the meantime
        return managers.apiManager.setUser((answer.authorization as any).user).then(() => toIm());
      }
      navigate(sentCodeToCardSpec(withPhoneNumber(answer, phone_number)));
    }).catch((err: ApiError) => {
      setBusy(false);
      toastNew({langPackKey: 'Error.AnError'});
      console.error('waPairing: auth.resendCode error', err);
    });
  }

  function back() {
    stop();
    managers.appAccountManager.cancelCode(phone_number, phone_code_hash);
    navigate({name: 'signIn'});
  }

  function copy() {
    navigator.clipboard?.writeText(code.replace('-', '')).catch(() => {});
  }

  onMount(() => {
    tickTimer = window.setInterval(() => {
      const s = Math.max(0, Math.round((deadline - Date.now()) / 1000));
      setLeft(s);
      if(!s) setExpired(true); // the server expires it too; the next poll would only confirm
    }, 1000);
    poll();
  });

  onCleanup(stop);

  const mmss = () => `${Math.floor(left() / 60)}:${String(left() % 60).padStart(2, '0')}`;

  return (
    <AuthCard
      inputWrapper={false}
      header={
        <MediaHeader>
          <MediaHeader.Sticker
            class={styles.logoContainer}
            size={120}
            element={
              <svg class={styles.logo} xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160">
                <use href="#logo"/>
              </svg>
            }
          />
          <MediaHeader.Title tag="h1">Link with WhatsApp</MediaHeader.Title>
          <MediaHeader.Subtitle class="secondary">{phone_number}</MediaHeader.Subtitle>
        </MediaHeader>
      }
    >
      <div class="input-wrapper">
        <Show when={!expired()} fallback={
          <>
            <div class="secondary" style={{'text-align': 'center'}}>This code has expired.</div>
            <Button class="btn-primary btn-color-primary" disabled={busy()} onClick={newCode}>Get a new code</Button>
          </>
        }>
          <div
            role="button"
            title="Click to copy"
            onClick={copy}
            style={{
              'font-family': 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
              'font-size': '2.25rem',
              'font-weight': '600',
              'letter-spacing': '.15em',
              'text-align': 'center',
              'cursor': 'pointer',
              'user-select': 'all',
              'padding': '.5rem 0'
            }}
          >
            {code}
          </div>
          <div class="secondary" style={{'text-align': 'center'}}>Expires in {mmss()}</div>
          <ol class={styles.qrDescription}>
            <li class={styles.qrDescriptionItem}><span class={styles.qrDescriptionMarker}>1</span>Open WhatsApp on your phone</li>
            <li class={styles.qrDescriptionItem}><span class={styles.qrDescriptionMarker}>2</span>Settings → Linked devices → Link a device</li>
            <li class={styles.qrDescriptionItem}><span class={styles.qrDescriptionMarker}>3</span>Tap “Link with phone number instead” and enter this code</li>
          </ol>
        </Show>
        <Button class="btn-primary btn-secondary btn-primary-transparent primary" onClick={back}>Use a different number</Button>
      </div>
    </AuthCard>
  );
}
