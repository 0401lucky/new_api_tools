import { useEffect, useState } from 'react'
import { Ticket, Plus, Clock } from 'lucide-react'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'
import { Generator } from './Generator'
import { Redemptions } from './Redemptions'
import { History } from './History'

type View = 'manage' | 'generator' | 'history'

const validViews: View[] = ['manage', 'generator', 'history']

function getInitialView(): View {
  const view = new URLSearchParams(window.location.search).get('view')
  if (view && (validViews as string[]).includes(view)) {
    return view as View
  }
  return 'manage'
}

function syncViewToURL(view: View) {
  const params = new URLSearchParams(window.location.search)
  if (view === 'manage') {
    params.delete('view')
  } else {
    params.set('view', view)
  }
  const query = params.toString()
  const next = `${window.location.pathname}${query ? `?${query}` : ''}`
  window.history.replaceState(null, '', next)
}

export function RedemptionCenter() {
  const [view, setView] = useState<View>(() => getInitialView())

  useEffect(() => {
    syncViewToURL(view)
  }, [view])

  return (
    <Tabs value={view} onValueChange={(value) => setView(value as View)} className="min-w-0 space-y-4">
      <TabsList className="grid h-auto w-full min-w-0 grid-cols-3 md:w-auto md:inline-flex">
        <TabsTrigger value="manage" className="min-w-0 gap-1.5 px-2 text-xs sm:gap-2 sm:px-3 sm:text-sm">
          <Ticket className="h-4 w-4 shrink-0" />
          <span className="truncate">兑换码</span>
        </TabsTrigger>
        <TabsTrigger value="generator" className="min-w-0 gap-1.5 px-2 text-xs sm:gap-2 sm:px-3 sm:text-sm">
          <Plus className="h-4 w-4 shrink-0" />
          <span className="truncate">生成器</span>
        </TabsTrigger>
        <TabsTrigger value="history" className="min-w-0 gap-1.5 px-2 text-xs sm:gap-2 sm:px-3 sm:text-sm">
          <Clock className="h-4 w-4 shrink-0" />
          <span className="truncate">生成记录</span>
        </TabsTrigger>
      </TabsList>

      <TabsContent value="manage" className="mt-0 min-w-0">
        <Redemptions />
      </TabsContent>
      <TabsContent value="generator" className="mt-0 min-w-0">
        <Generator />
      </TabsContent>
      <TabsContent value="history" className="mt-0 min-w-0">
        <History />
      </TabsContent>
    </Tabs>
  )
}
