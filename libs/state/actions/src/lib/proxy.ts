import { ErrorHandler } from '@angular/core';
import {
  BehaviorSubject,
  distinctUntilChanged,
  filter,
  from,
  map,
  merge,
  Observable,
  OperatorFunction,
  share,
  Subject,
  tap,
} from 'rxjs';
import {
  ActionContext,
  ActionStatus,
  EffectMap,
  KeysOf,
  RxActions,
  SubjectMap,
  ValuesOf,
} from './types';

/**
 * @internal
 * Internal helper to create the proxy object
 * It lives as standalone function because we don't need to carrie it in memory for every ActionHandler instance
 * @param subjects
 * @param transforms
 */
export function actionProxyHandler<T extends object, U extends object>({
  subjectMap,
  transformsMap,
  effectMap,
  errorHandler = null,
}: {
  subjectMap: SubjectMap<T>;
  transformsMap?: U;
  effectMap: EffectMap<T>;
  errorHandler: ErrorHandler | null;
}): ProxyHandler<RxActions<T, U>> {
  type KeysOfT = KeysOf<T>;
  type ValuesOfT = ValuesOf<T>;

  function getEventEmitter(prop: KeysOfT): Subject<ActionContext<ValuesOfT>> {
    if (!subjectMap[prop]) {
      subjectMap[prop] = new Subject<ActionContext<ValuesOfT>>();
    }
    return subjectMap[prop];
  }

  // track streams for each action
  const actionStreams = new Map<
    string,
    {
      action$: Observable<any>;
      loading$: BehaviorSubject<boolean>;
      complete$: Subject<any>;
      error$: BehaviorSubject<boolean>;
    }
  >();

  function getOrCreateActionStreams(prop: string) {
    if (!actionStreams.has(prop)) {
      const emitter = getEventEmitter(prop as KeysOfT);
      const loading$ = new BehaviorSubject(false);
      const error$ = new BehaviorSubject(false);
      const complete$ = new Subject<any>();

      const action$ = emitter.pipe(
        tap((ctx: ActionContext<ValuesOfT>) => {
          switch (ctx.status) {
            case ActionStatus.Dispatched:
              loading$.next(true);
              error$.next(false);
              break;
            case ActionStatus.Completed:
              loading$.next(false);
              complete$.next(ctx.result);
              break;
            case ActionStatus.Errored:
              loading$.next(false);
              error$.next(true);
              if (ctx.error && errorHandler) {
                errorHandler.handleError(ctx.error);
              }
              break;
          }
        }),
        share(),
      );

      actionStreams.set(prop, {
        action$,
        loading$,
        complete$,
        error$,
      });
    }

    const propStream = actionStreams.get(prop);
    if (propStream === undefined) {
      throw new Error(`Action stream not found for prop '${prop}`);
    }

    return propStream;
  }

  function dispatch(value: ValuesOfT, prop: KeysOfT) {
    const emitter = getEventEmitter(prop);

    try {
      // Signal start of action
      emitter.next({
        action: value,
        status: ActionStatus.Dispatched,
      } as ActionContext<ValuesOfT>);

      const transformedValue =
        transformsMap && (transformsMap as any)[prop]
          ? (transformsMap as any)[prop](value)
          : value;

      if (
        transformedValue &&
        typeof transformedValue === 'object' &&
        'pipe' in transformedValue
      ) {
        // Handle async (Observable) results
        from(transformedValue)
          .pipe(
            tap({
              next: (result) => {
                emitter.next({
                  action: result,
                  status: ActionStatus.Completed,
                  result,
                } as ActionContext<ValuesOfT>);
              },
              error: (error) => {
                emitter.next({
                  action: value,
                  status: ActionStatus.Errored,
                  error,
                } as ActionContext<ValuesOfT>);
              },
            }),
          )
          .subscribe();
      } else {
        // Handle sync results immediately
        emitter.next({
          action: value,
          status: ActionStatus.Completed,
          result: transformedValue,
        } as ActionContext<ValuesOfT>);
      }
    } catch (error) {
      emitter.next({
        action: value,
        status: ActionStatus.Errored,
        error,
      } as ActionContext<ValuesOfT>);
      if (errorHandler) {
        errorHandler.handleError(error);
      }
    }
  }

  return {
    apply(_: RxActions<T, U>, __: any, props: [T]): any {
      props.forEach((slice) =>
        Object.entries(slice).forEach(([k, v]) =>
          dispatch(v as any, k as any as KeysOfT),
        ),
      );
    },

    get(_, property: string) {
      // Special handling for the $ property
      if (property === '$') {
        return (props: (keyof T)[]) =>
          merge(
            ...props.map((k) =>
              getEventEmitter(k as KeysOfT).pipe(
                filter((ctx) => ctx.status === ActionStatus.Completed),
                map((ctx) => ctx.action),
              ),
            ),
          );
      }

      if (property.endsWith('$')) {
        if (property.endsWith('Loading$')) {
          const baseProp = property.slice(0, -8);
          return getOrCreateActionStreams(baseProp).loading$;
        }

        if (property.endsWith('Complete$')) {
          const baseProp = property.slice(0, -9);
          return getOrCreateActionStreams(baseProp).complete$;
        }

        if (property.endsWith('Error$')) {
          const baseProp = property.slice(0, -6);
          return getOrCreateActionStreams(baseProp).error$;
        }

        // Regular action$ observable
        const baseProp = property.slice(0, -1) as KeysOfT;
        return getOrCreateActionStreams(baseProp as string).action$.pipe(
          map((ctx: ActionContext<ValuesOfT>) =>
            'result' in ctx ? ctx.result : ctx.action,
          ),
          distinctUntilChanged(),
        );
      }

      if (property.startsWith('on')) {
        const slicedPropName = property.toString().slice(2);
        const propName = (slicedPropName.charAt(0).toLowerCase() +
          slicedPropName.slice(1)) as KeysOfT;
        return (
          behaviour: OperatorFunction<T[KeysOfT], T[KeysOfT]>,
          sf: (v: T[KeysOfT]) => void,
        ) => {
          const sub = getEventEmitter(propName)
            .pipe(
              // only take actions that have completed to avoid multiple emissions
              filter((ctx) => ctx.status === ActionStatus.Completed),
              map((ctx) => ctx.action),
              behaviour,
            )
            .subscribe(sf);
          effectMap[propName] = sub;
          return () => sub.unsubscribe();
        };
      }

      return (args: ValuesOfT) => {
        dispatch(args, property as KeysOfT);
      };
    },

    set(): boolean {
      throw new Error('No setters available. To emit call the property name.');
    },
  };
}
