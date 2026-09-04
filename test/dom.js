// dom.js - A DOM on the globals for the framework tests (happy-dom); import it first
import { GlobalRegistrator } from '@happy-dom/global-registrator';

GlobalRegistrator.register({ url: 'http://localhost' });
// React's act() wants to be told it runs in a test
globalThis.IS_REACT_ACT_ENVIRONMENT = true;
