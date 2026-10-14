import React, { useState } from 'react';
import { StreamProvider } from './context/StreamContext';
import { Header, type TabKey } from './components/Header';
import { EventTicker } from './components/EventTicker';
import { OverviewTab } from './components/OverviewTab';
import { LiveTab } from './components/LiveTab';
import { HdfsTab } from './components/HdfsTab';
import { KafkaTab } from './components/KafkaTab';
import { SparkTab } from './components/SparkTab';
import { AqiTab } from './components/AqiTab';
import { FlowsTab } from './components/FlowsTab';
import { SqlTab } from './components/SqlTab';
import { PredictTab } from './components/PredictTab';
import { TerminalTab } from './components/TerminalTab';

const AppContent: React.FC = () => {
  const [currentTab, setCurrentTab] = useState<TabKey>('live');

  return (
    <div className="min-h-screen flex flex-col bg-slate-950 text-slate-100 font-sans selection:bg-sky-500 selection:text-white">
      {/* Header và Ticker */}
      <Header currentTab={currentTab} onTabChange={setCurrentTab} />
      <EventTicker />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
        {currentTab === 'live' && <LiveTab />}
        {currentTab === 'overview' && <OverviewTab />}
        {currentTab === 'hdfs' && <HdfsTab />}
        {currentTab === 'kafka' && <KafkaTab />}
        {currentTab === 'spark' && <SparkTab />}
        {currentTab === 'aqi' && <AqiTab />}
        {currentTab === 'flows' && <FlowsTab />}
        {currentTab === 'sql' && <SqlTab />}
        {currentTab === 'predict' && <PredictTab />}
        {currentTab === 'terminal' && <TerminalTab />}
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800 bg-slate-900/60 py-6 text-xs text-slate-400 mt-auto">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between gap-4 text-center sm:text-left">
          <div>
            <span className="font-semibold text-slate-300">
              Đồ án Big Data Giám sát Khí CO TP.HCM
            </span>{' '}
            • Hadoop 3.3.6 • Apache Kafka • Apache Spark
          </div>
          <div className="text-slate-500 font-mono text-[11px]">
            Frontend: Vite + React + TypeScript • Hợp đồng OpenAPI 3.1
          </div>
        </div>
      </footer>
    </div>
  );
};

export function App() {
  return (
    <StreamProvider>
      <AppContent />
    </StreamProvider>
  );
}

export default App;
