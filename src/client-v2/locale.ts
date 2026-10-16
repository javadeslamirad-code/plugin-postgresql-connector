import { useFlowEngine } from '@nocobase/flow-engine';
// @ts-ignore
import pkg from './../../package.json';

export function useT() {
  const engine = useFlowEngine();
  return (key: string) => engine.context.t(key, { ns: [pkg.name, 'client'] });
}
