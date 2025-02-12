import { Component, ErrorHandler, Provider } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { isObservable, of, throwError, timer } from 'rxjs';
import { delay, map, switchMap, tap } from 'rxjs/operators';
import { rxActions } from './rx-actions';
import { ActionTransforms } from './types';

describe('actions fn', () => {
  it('should get created properly', () => {
    const { component } = setupComponent<Actions>();
    expect(typeof component.actions.prop).toBe('function');
    expect(isObservable(component.actions.prop)).toBeFalsy();
    expect(isObservable(component.actions.prop$)).toBeTruthy();
    expect(typeof component.actions.onProp).toBe('function');
  });

  it('should emit on the subscribed channels', (done) => {
    const values = 'foo';
    const exp = values;
    const { component } = setupComponent<Actions>();
    component.actions.prop$.subscribe((result) => {
      expect(result).toBe(exp);
      done();
    });
    component.actions.prop(values);
  });

  it('should maintain channels per create call', (done) => {
    const values = 'foo';
    const nextSpy = jest.fn((_: string) => void 0);
    const exp = values;
    const { component } = setupComponent<Actions>();
    component.actions2.prop$.subscribe(nextSpy);
    component.actions.prop$.subscribe((result) => {
      expect(result).toBe(exp);
      done();
    });
    expect(nextSpy).not.toHaveBeenCalled();
    component.actions.prop(values);
  });

  it('should emit and transform on the subscribed channels', (done) => {
    const exp = 'transformed';
    const { component } = setupComponent<Actions>({
      transformFns: { prop: (x: any) => 'transformed' },
    });
    component.actions.prop$.subscribe((result) => {
      expect(result).toBe(exp);
      done();
    });
    component.actions.prop('');
  });

  it('should emit on multiple subscribed channels', (done) => {
    const value1 = 'foo';
    const value2 = 'bar';
    const res = {};
    const { component } = setupComponent<Actions>();

    component.actions.prop$.subscribe((result) => {
      res['prop'] = result;
    });
    component.actions.prop2$.subscribe((result) => {
      res['prop2'] = result;
    });
    component.actions({ prop: value1, prop2: value2 });
    expect(res).toStrictEqual({ prop: value1, prop2: value2 });
    done();
  });

  it('should emit on multiple subscribed channels over merged output', (done) => {
    const value1 = 'foo';
    const value2 = 'bar';

    const res = [];
    const { component } = setupComponent<Actions>();

    expect(typeof component.actions.$).toBe('function');
    component.actions.$(['prop', 'prop2']).subscribe((result) => {
      res.push(result);
    });
    component.actions({ prop: value1, prop2: value2 });
    expect(res.length).toBe(2);
    expect(res).toStrictEqual([value1, value2]);
    done();
  });

  it('should trigger side effect', () => {
    const { component } = setupComponent<Actions>();
    const t = { se: () => void 0 };
    const dummyBehaviour = (o$) => o$;
    const spyT = jest.fn((_: any) => void 0);
    const spyF = jest.fn((_: any) => void 0);

    component.actions.onProp(dummyBehaviour, spyT);
    component.actions.prop('p');
    expect(spyT).toBeCalledTimes(1);
    expect(spyT).toBeCalledWith('p');

    component.actions.onLongPropName(dummyBehaviour, spyF);
    component.actions.longPropName('p');
    expect(spyF).toBeCalledTimes(1);
    expect(spyF).toBeCalledWith('p');
  });

  it('should apply behaviour to trigger', () => {
    const { component } = setupComponent<Actions>();
    const t = { se: () => void 0 };
    const spyBehavior = jest.fn();
    const dummyBehaviour = (o$) => o$.pipe(tap(spyBehavior));

    const sub = component.actions.onProp(dummyBehaviour);
    component.actions.prop('p');
    expect(spyBehavior).toHaveBeenCalledTimes(1);
    sub();
    component.actions.prop('p');
    expect(spyBehavior).toHaveBeenCalledTimes(1);
  });

  it('should not trigger side effect after unsubscribed', () => {
    const { component, fixture } = setupComponent<Actions>();
    const t = { se: () => void 0 };
    const dummyBehaviour = (o$) => o$;
    const spyT = jest.fn((_: any) => void 0);

    const unsub = component.actions.onProp(dummyBehaviour, spyT);
    unsub(); // stop listening to the emissions
    component.actions.prop('p');
    expect(spyT).toBeCalledTimes(0);
  });

  it('should destroy all created actions and subscriptions on component destroy', (done) => {
    const dummyBehaviour = (o$) => o$;
    const spyEmission = jest.fn((_: any) => void 0);
    const spyEffect = jest.fn((_: any) => void 0);
    const spyEmission2 = jest.fn((_: any) => void 0);
    const spyEffect2 = jest.fn((_: any) => void 0);

    const { component, fixture } = setupComponent<Actions>();
    component.actions.prop$.subscribe(spyEmission);
    component.actions2.prop$.subscribe(spyEmission2);

    const ef = component.actions.onProp(dummyBehaviour, spyEffect);
    const ef2 = component.actions2.onProp(dummyBehaviour, spyEffect2);

    expect(spyEmission).toBeCalledTimes(0);
    expect(spyEffect).toBeCalledTimes(0);

    expect(spyEmission2).toBeCalledTimes(0);
    component.actions.prop('');
    component.actions2.prop('');
    expect(spyEmission).toBeCalledTimes(1);
    expect(spyEffect).toBeCalledTimes(1);

    expect(spyEmission2).toBeCalledTimes(1);
    expect(spyEffect2).toBeCalledTimes(1);

    fixture.destroy();
    component.actions.prop('');
    component.actions2.prop('');
    expect(spyEmission).toBeCalledTimes(1);
    expect(spyEffect).toBeCalledTimes(1);

    expect(spyEmission2).toBeCalledTimes(1);
    expect(spyEffect2).toBeCalledTimes(1);

    done();
  });

  it('should throw if a setter is used', (done) => {
    const { component } = setupComponent<Actions>();
    expect(() => {
      (component.actions as any).prop = 0;
    }).toThrow('');

    done();
  });

  it('should isolate errors and invoke provided ErrorHandler', () => {
    const customErrorHandler: ErrorHandler = {
      handleError: jest.fn(),
    };
    const { fixture } = setupComponent<Actions>({
      transformFns: {
        resize: (_: string | number): number => {
          throw new Error('something went wrong');
        },
      },
      providers: [
        {
          provide: ErrorHandler,
          useValue: customErrorHandler,
        },
      ],
    });
    fixture.componentInstance.actions.search('');
    fixture.componentInstance.actions.resize(42);

    expect(customErrorHandler.handleError).toHaveBeenCalledWith(
      new Error('something went wrong'),
    );
  });

  it('should throw if called outside of injection context', () => {
    expect(() => rxActions<Actions>()).toThrow('');
  });
});

describe('rxActions - Action Status Streams', () => {
  describe('Loading State', () => {
    it('should emit loading states for sync operations', (done) => {
      const { component } = setupComponent<{ process: string }>();
      const loadingStates: boolean[] = [];

      component.actions.processLoading$.subscribe((loading) => {
        loadingStates.push(loading);
        if (loadingStates.length === 2) {
          expect(loadingStates).toEqual([true, false]);
          done();
        }
      });

      component.actions.process('test');
    });

    it('should track loading state for async operations', (done) => {
      const { component } = setupComponent<{ fetch: void }>({
        transformFns: {
          fetch: () => timer(50).pipe(map(() => 'done')),
        },
      });

      const loadingStates: boolean[] = [];
      component.actions.fetchLoading$.subscribe((loading) => {
        loadingStates.push(loading);
        if (loadingStates.length === 2) {
          expect(loadingStates).toEqual([true, false]);
          done();
        }
      });

      component.actions.fetch();
    });
  });

  describe('Completion State', () => {
    it('should emit result for sync operations', (done) => {
      const { component } = setupComponent<{ process: string }>();

      component.actions.processComplete$.subscribe((result) => {
        expect(result).toBe('test');
        done();
      });

      component.actions.process('test');
    });

    it('should emit transformed result for async operations', (done) => {
      const { component } = setupComponent<{ fetch: void }>({
        transformFns: {
          fetch: () => of('async result').pipe(delay(50)),
        },
      });

      component.actions.fetchComplete$.subscribe((result) => {
        expect(result).toBe('async result');
        done();
      });

      component.actions.fetch();
    });

    it('should handle chained async operations', (done) => {
      const { component } = setupComponent<{ process: string }>({
        transformFns: {
          process: (input: string) =>
            of(input).pipe(
              delay(50),
              map((str) => str.toUpperCase()),
              delay(50),
            ),
        },
      });

      component.actions.processComplete$.subscribe((result) => {
        expect(result).toBe('TEST');
        done();
      });

      component.actions.process('test');
    });
  });

  describe('Error State', () => {
    it('should handle sync errors', (done) => {
      const { component } = setupComponent<{ process: string }>({
        transformFns: {
          process: () => {
            throw new Error('sync error');
          },
        },
      });

      component.actions.processError$.subscribe((hasError) => {
        expect(hasError).toBe(true);
        done();
      });

      component.actions.process('test');
    });

    it('should handle async errors', (done) => {
      const { component } = setupComponent<{ fetch: void }>({
        transformFns: {
          fetch: () =>
            timer(50).pipe(
              switchMap(() => throwError(() => new Error('async error'))),
            ),
        },
      });

      component.actions.fetchError$.subscribe((hasError) => {
        expect(hasError).toBe(true);
        done();
      });

      component.actions.fetch();
    });

    it('should propagate error to error handler', () => {
      const errorHandler = { handleError: jest.fn() };
      const error = new Error('test error');

      const { component } = setupComponent<{ fetch: void }>({
        transformFns: {
          fetch: () => throwError(() => error),
        },
        providers: [{ provide: ErrorHandler, useValue: errorHandler }],
      });

      component.actions.fetch();
      expect(errorHandler.handleError).toHaveBeenCalledWith(error);
    });

    it('should reset error state on successful operation', (done) => {
      const { component } = setupComponent<{ fetch: void }>({
        transformFns: {
          fetch: () => of('success'),
        },
      });

      let errorSeen = false;
      component.actions.fetchError$.subscribe((hasError) => {
        if (!errorSeen) {
          expect(hasError).toBe(false);
          errorSeen = true;
          done();
        }
      });

      component.actions.fetch();
    });
  });

  describe('Multiple Concurrent Actions', () => {
    it('should maintain independent states for different actions', (done) => {
      const { component } = setupComponent<{
        fastAction: void;
        slowAction: void;
      }>({
        transformFns: {
          fastAction: () => of('fast').pipe(delay(50)),
          slowAction: () => of('slow').pipe(delay(100)),
        },
      });

      const states = {
        fastLoading: [] as boolean[],
        slowLoading: [] as boolean[],
        completed: [] as string[],
      };

      component.actions.fastActionLoading$.subscribe((loading) => {
        states.fastLoading.push(loading);
      });

      component.actions.slowActionLoading$.subscribe((loading) => {
        states.slowLoading.push(loading);
      });

      let completedCount = 0;
      component.actions.fastActionComplete$.subscribe((result) => {
        states.completed.push('fast');
        checkComplete();
      });

      component.actions.slowActionComplete$.subscribe((result) => {
        states.completed.push('slow');
        checkComplete();
      });

      component.actions.fastAction();
      component.actions.slowAction();

      function checkComplete() {
        completedCount++;
        if (completedCount === 2) {
          expect(states.fastLoading).toEqual([true, false]);
          expect(states.slowLoading).toEqual([true, false]);
          expect(states.completed).toEqual(['fast', 'slow']);
          done();
        }
      }
    });
  });
});

type Actions = {
  prop: string;
  prop2: string;
  search: string;
  resize: number;
  longPropName: string;
};
function setupComponent<
  Actions extends object,
  Transforms extends ActionTransforms<Actions> = object,
>(cfg?: { transformFns?: Transforms; providers?: Provider[] }) {
  let providers = [];
  if (Array.isArray(cfg?.providers)) {
    providers = cfg.providers;
  }
  @Component({
    template: '',
    providers,
  })
  class TestComponent {
    actions = rxActions<Actions>(({ transforms }) => {
      if (cfg?.transformFns) {
        transforms(cfg.transformFns);
      }
    });
    actions2 = rxActions<Actions>(({ transforms }) => {
      if (cfg?.transformFns) {
        transforms(cfg.transformFns);
      }
    });
  }

  TestBed.configureTestingModule({
    imports: [TestComponent],
  });

  const fixture = TestBed.createComponent(TestComponent);
  const component = fixture.componentInstance;

  return { component, fixture };
}
