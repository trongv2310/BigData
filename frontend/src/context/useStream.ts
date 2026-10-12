import { useContext } from 'react';
import { StreamContext } from './streamContextDef';

export const useStream = () => useContext(StreamContext);
