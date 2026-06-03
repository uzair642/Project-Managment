var yA=Object.defineProperty,bA=Object.defineProperties;var wA=Object.getOwnPropertyDescriptors;var Sd=Object.getOwnPropertySymbols;var Y0=Object.prototype.hasOwnProperty,K0=Object.prototype.propertyIsEnumerable;var Q0=(t,n,e)=>n in t?yA(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,w=(t,n)=>{for(var e in n||={})Y0.call(n,e)&&Q0(t,e,n[e]);if(Sd)for(var e of Sd(n))K0.call(n,e)&&Q0(t,e,n[e]);return t},oe=(t,n)=>bA(t,wA(n));var mp=(t,n)=>{var e={};for(var i in t)Y0.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&Sd)for(var i of Sd(t))n.indexOf(i)<0&&K0.call(t,i)&&(e[i]=t[i]);return e};var jt=null,Id=!1,hp=1,CA=null,dt=Symbol("SIGNAL");function ie(t){let n=jt;return jt=t,n}function kd(){return jt}var Pr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Lr(t){if(Id)throw new Error("");if(jt===null)return;jt.consumerOnSignalRead(t);let n=jt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=jt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:jt.producers,e!==void 0&&e.producer===t)){jt.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===jt&&(!i||xA(r,jt)))return;let o=As(jt),s={producer:t,consumer:jt,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};jt.producersTail=s,n!==void 0?n.nextProducer=s:jt.producers=s,o&&ew(t,s)}function Z0(){hp++}function So(t){if(!(As(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===hp)){if(!t.producerMustRecompute(t)&&!Ts(t)){Ms(t);return}t.producerRecomputeValue(t),Ms(t)}}function pp(t){if(t.consumers===void 0)return;let n=Id;Id=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||DA(i)}}finally{Id=n}}function gp(){return jt?.consumerAllowSignalWrites!==!1}function DA(t){t.dirty=!0,pp(t),t.consumerMarkedDirty?.(t)}function Ms(t){t.dirty=!1,t.lastCleanEpoch=hp}function Ji(t){return t&&X0(t),ie(t)}function X0(t){t.producersTail=void 0,t.recomputing=!0}function Br(t,n){ie(n),t&&J0(t)}function J0(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(As(t))do e=_p(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Ts(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(So(e),i!==e.version))return!0}return!1}function Vr(t){if(As(t)){let n=t.producers;for(;n!==void 0;)n=_p(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function ew(t,n){let e=t.consumersTail,i=As(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)ew(r.producer,r)}function _p(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!As(n)){let o=n.producers;for(;o!==void 0;)o=_p(o)}return e}function As(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function tl(t){CA?.(t)}function xA(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function nl(t,n){return Object.is(t,n)}function il(t,n){let e=Object.create(EA);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(So(e),Lr(e),e.value===xi)throw e.error;return e.value};return i[dt]=e,tl(e),i}var xo=Symbol("UNSET"),Eo=Symbol("COMPUTING"),xi=Symbol("ERRORED"),EA=oe(w({},Pr),{value:xo,dirty:!0,error:null,equal:nl,kind:"computed",producerMustRecompute(t){return t.value===xo||t.value===Eo},producerRecomputeValue(t){if(t.value===Eo)throw new Error("");let n=t.value;t.value=Eo;let e=Ji(t),i,r=!1;try{i=t.computation(),ie(null),r=n!==xo&&n!==xi&&i!==xi&&t.equal(n,i)}catch(o){i=xi,t.error=o}finally{Br(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function SA(){throw new Error}var tw=SA;function nw(t){tw(t)}function vp(t){tw=t}var IA=null;function yp(t,n){let e=Object.create(rl);e.value=t,n!==void 0&&(e.equal=n);let i=()=>iw(e);return i[dt]=e,tl(e),[i,s=>Io(e,s),s=>Md(e,s)]}function iw(t){return Lr(t),t.value}function Io(t,n){gp()||nw(t),t.equal(t.value,n)||(t.value=n,kA(t))}function Md(t,n){gp()||nw(t),Io(t,n(t.value))}var rl=oe(w({},Pr),{equal:nl,value:void 0,kind:"signal"});function kA(t){t.version++,Z0(),pp(t),IA?.(t)}var bp=oe(w({},Pr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function wp(t){if(t.dirty=!1,t.version>0&&!Ts(t))return;t.version++;let n=Ji(t);try{t.cleanup(),t.fn()}finally{Br(t,n)}}function we(t){return typeof t=="function"}function Rs(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Td=Rs(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function ko(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var Se=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(we(i))try{i()}catch(o){n=o instanceof Td?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{rw(o)}catch(s){n=n??[],s instanceof Td?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Td(n)}}add(n){var e;if(n&&n!==this)if(this.closed)rw(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&ko(e,n)}remove(n){let{_finalizers:e}=this;e&&ko(e,n),n instanceof t&&n._removeParent(this)}};Se.EMPTY=(()=>{let t=new Se;return t.closed=!0,t})();var Cp=Se.EMPTY;function Ad(t){return t instanceof Se||t&&"closed"in t&&we(t.remove)&&we(t.add)&&we(t.unsubscribe)}function rw(t){we(t)?t():t.unsubscribe()}var Kn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Os={setTimeout(t,n,...e){let{delegate:i}=Os;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Os;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Rd(t){Os.setTimeout(()=>{let{onUnhandledError:n}=Kn;if(n)n(t);else throw t})}function ol(){}var ow=Dp("C",void 0,void 0);function sw(t){return Dp("E",void 0,t)}function aw(t){return Dp("N",t,void 0)}function Dp(t,n,e){return{kind:t,value:n,error:e}}var Mo=null;function Ns(t){if(Kn.useDeprecatedSynchronousErrorHandling){let n=!Mo;if(n&&(Mo={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Mo;if(Mo=null,e)throw i}}else t()}function lw(t){Kn.useDeprecatedSynchronousErrorHandling&&Mo&&(Mo.errorThrown=!0,Mo.error=t)}var To=class extends Se{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Ad(n)&&n.add(this)):this.destination=AA}static create(n,e,i){return new er(n,e,i)}next(n){this.isStopped?Ep(aw(n),this):this._next(n)}error(n){this.isStopped?Ep(sw(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Ep(ow,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},MA=Function.prototype.bind;function xp(t,n){return MA.call(t,n)}var Sp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){Od(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){Od(i)}else Od(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){Od(e)}}},er=class extends To{constructor(n,e,i){super();let r;if(we(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Kn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&xp(n.next,o),error:n.error&&xp(n.error,o),complete:n.complete&&xp(n.complete,o)}):r=n}this.destination=new Sp(r)}};function Od(t){Kn.useDeprecatedSynchronousErrorHandling?lw(t):Rd(t)}function TA(t){throw t}function Ep(t,n){let{onStoppedNotification:e}=Kn;e&&Os.setTimeout(()=>e(t,n))}var AA={closed:!0,next:ol,error:TA,complete:ol};var Fs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Dn(t){return t}function Ip(...t){return kp(t)}function kp(t){return t.length===0?Dn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var de=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=OA(e)?e:new er(e,i,r);return Ns(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=cw(i),new i((r,o)=>{let s=new er({next:a=>{try{e(a)}catch(l){o(l),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Fs](){return this}pipe(...e){return kp(e)(this)}toPromise(e){return e=cw(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function cw(t){var n;return(n=t??Kn.Promise)!==null&&n!==void 0?n:Promise}function RA(t){return t&&we(t.next)&&we(t.error)&&we(t.complete)}function OA(t){return t&&t instanceof To||RA(t)&&Ad(t)}function Mp(t){return we(t?.lift)}function ve(t){return n=>{if(Mp(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function pe(t,n,e,i,r){return new Tp(t,n,e,i,r)}var Tp=class extends To{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function dw(){return ve((t,n)=>{let e=null;t._refCount++;let i=pe(n,void 0,void 0,void 0,()=>{if(!t||t._refCount<=0||0<--t._refCount){e=null;return}let r=t._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),n.unsubscribe()});t.subscribe(i),i.closed||(e=t.connect())})}var sl=class extends de{constructor(n,e){super(),this.source=n,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,Mp(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new Se;let e=this.getSubject();n.add(this.source.subscribe(pe(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),n.closed&&(this._connection=null,n=Se.EMPTY)}return n}refCount(){return dw()(this)}};var Ps={schedule(t){let n=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=Ps;i&&(n=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=n(o=>{e=void 0,t(o)});return new Se(()=>e?.(r))},requestAnimationFrame(...t){let{delegate:n}=Ps;return(n?.requestAnimationFrame||requestAnimationFrame)(...t)},cancelAnimationFrame(...t){let{delegate:n}=Ps;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...t)},delegate:void 0};var uw=Rs(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var I=(()=>{class t extends de{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Nd(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new uw}next(e){Ns(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Ns(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Ns(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Cp:(this.currentObservers=null,o.push(e),new Se(()=>{this.currentObservers=null,ko(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new de;return e.source=this,e}}return t.create=(n,e)=>new Nd(n,e),t})(),Nd=class extends I{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Cp}};var qe=class extends I{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var al={now(){return(al.delegate||Date).now()},delegate:void 0};var An=class extends I{constructor(n=1/0,e=1/0,i=al){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var Fd=class extends Se{constructor(n,e){super()}schedule(n,e=0){return this}};var ll={setInterval(t,n,...e){let{delegate:i}=ll;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=ll;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var jr=class extends Fd{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return ll.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&ll.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,ko(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var NA=1,Ap,Rp={};function fw(t){return t in Rp?(delete Rp[t],!0):!1}var mw={setImmediate(t){let n=NA++;return Rp[n]=!0,Ap||(Ap=Promise.resolve()),Ap.then(()=>fw(n)&&t()),n},clearImmediate(t){fw(t)}};var{setImmediate:FA,clearImmediate:PA}=mw,cl={setImmediate(...t){let{delegate:n}=cl;return(n?.setImmediate||FA)(...t)},clearImmediate(t){let{delegate:n}=cl;return(n?.clearImmediate||PA)(t)},delegate:void 0};var Pd=class extends jr{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=cl.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(cl.clearImmediate(e),n._scheduled===e&&(n._scheduled=void 0))}};var Ls=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Ls.now=al.now;var zr=class extends Ls{constructor(n,e=Ls.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Ld=class extends zr{flush(n){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var Bd=new Ld(Pd);var dl=new zr(jr),hw=dl;var Vd=class extends jr{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=Ps.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&e===n._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(Ps.cancelAnimationFrame(e),n._scheduled=void 0)}};var jd=class extends zr{flush(n){this._active=!0;let e;n?e=n.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var zd=new jd(Vd);var ut=new de(t=>t.complete());function Ud(t){return t&&we(t.schedule)}function Op(t){return t[t.length-1]}function Hd(t){return we(Op(t))?t.pop():void 0}function Ei(t){return Ud(Op(t))?t.pop():void 0}function pw(t,n){return typeof Op(t)=="number"?t.pop():n}function _w(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{c(i.next(u))}catch(f){s(f)}}function l(u){try{c(i.throw(u))}catch(f){s(f)}}function c(u){u.done?o(u.value):r(u.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function gw(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Ao(t){return this instanceof Ao?(this.v=t,this):new Ao(t)}function vw(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(m){return function(_){return Promise.resolve(_).then(m,f)}}function a(m,_){i[m]&&(r[m]=function(D){return new Promise(function(E,M){o.push([m,D,E,M])>1||l(m,D)})},_&&(r[m]=_(r[m])))}function l(m,_){try{c(i[m](_))}catch(D){h(o[0][3],D)}}function c(m){m.value instanceof Ao?Promise.resolve(m.value.v).then(u,f):h(o[0][2],m)}function u(m){l("next",m)}function f(m){l("throw",m)}function h(m,_){m(_),o.shift(),o.length&&l(o[0][0],o[0][1])}}function yw(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof gw=="function"?gw(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var $d=t=>t&&typeof t.length=="number"&&typeof t!="function";function Gd(t){return we(t?.then)}function Wd(t){return we(t[Fs])}function qd(t){return Symbol.asyncIterator&&we(t?.[Symbol.asyncIterator])}function Qd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function LA(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Yd=LA();function Kd(t){return we(t?.[Yd])}function Zd(t){return vw(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Ao(e.read());if(r)return yield Ao(void 0);yield yield Ao(i)}}finally{e.releaseLock()}})}function Xd(t){return we(t?.getReader)}function Qe(t){if(t instanceof de)return t;if(t!=null){if(Wd(t))return BA(t);if($d(t))return VA(t);if(Gd(t))return jA(t);if(qd(t))return bw(t);if(Kd(t))return zA(t);if(Xd(t))return UA(t)}throw Qd(t)}function BA(t){return new de(n=>{let e=t[Fs]();if(we(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function VA(t){return new de(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function jA(t){return new de(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Rd)})}function zA(t){return new de(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function bw(t){return new de(n=>{HA(t,n).catch(e=>n.error(e))})}function UA(t){return bw(Zd(t))}function HA(t,n){var e,i,r,o;return _w(this,void 0,void 0,function*(){try{for(e=yw(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function mn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Jd(t,n=0){return ve((e,i)=>{e.subscribe(pe(i,r=>mn(i,t,()=>i.next(r),n),()=>mn(i,t,()=>i.complete(),n),r=>mn(i,t,()=>i.error(r),n)))})}function eu(t,n=0){return ve((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function ww(t,n){return Qe(t).pipe(eu(n),Jd(n))}function Cw(t,n){return Qe(t).pipe(eu(n),Jd(n))}function Dw(t,n){return new de(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function xw(t,n){return new de(e=>{let i;return mn(e,n,()=>{i=t[Yd](),mn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>we(i?.return)&&i.return()})}function tu(t,n){if(!t)throw new Error("Iterable cannot be null");return new de(e=>{mn(e,n,()=>{let i=t[Symbol.asyncIterator]();mn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Ew(t,n){return tu(Zd(t),n)}function Sw(t,n){if(t!=null){if(Wd(t))return ww(t,n);if($d(t))return Dw(t,n);if(Gd(t))return Cw(t,n);if(qd(t))return tu(t,n);if(Kd(t))return xw(t,n);if(Xd(t))return Ew(t,n)}throw Qd(t)}function Ge(t,n){return n?Sw(t,n):Qe(t)}function q(...t){let n=Ei(t);return Ge(t,n)}function ul(t,n){let e=we(t)?t:()=>t,i=r=>r.error(e());return new de(n?r=>n.schedule(i,0,r):i)}function Ro(t){return!!t&&(t instanceof de||we(t.lift)&&we(t.subscribe))}var Oo=Rs(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Iw(t){return t instanceof Date&&!isNaN(t)}function ee(t,n){return ve((e,i)=>{let r=0;e.subscribe(pe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:$A}=Array;function GA(t,n){return $A(n)?t(...n):t(n)}function nu(t){return ee(n=>GA(t,n))}var{isArray:WA}=Array,{getPrototypeOf:qA,prototype:QA,keys:YA}=Object;function iu(t){if(t.length===1){let n=t[0];if(WA(n))return{args:n,keys:null};if(KA(n)){let e=YA(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function KA(t){return t&&typeof t=="object"&&qA(t)===QA}function ru(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function hn(...t){let n=Ei(t),e=Hd(t),{args:i,keys:r}=iu(t);if(i.length===0)return Ge([],n);let o=new de(ZA(i,n,r?s=>ru(r,s):Dn));return e?o.pipe(nu(e)):o}function ZA(t,n,e=Dn){return i=>{kw(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)kw(n,()=>{let c=Ge(t[l],n),u=!1;c.subscribe(pe(i,f=>{o[l]=f,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function kw(t,n,e){t?mn(e,t,n):n()}function Mw(t,n,e,i,r,o,s,a){let l=[],c=0,u=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},m=D=>c<i?_(D):l.push(D),_=D=>{o&&n.next(D),c++;let E=!1;Qe(e(D,u++)).subscribe(pe(n,M=>{r?.(M),o?m(M):n.next(M)},()=>{E=!0},void 0,()=>{if(E)try{for(c--;l.length&&c<i;){let M=l.shift();s?mn(n,s,()=>_(M)):_(M)}h()}catch(M){n.error(M)}}))};return t.subscribe(pe(n,m,()=>{f=!0,h()})),()=>{a?.()}}function Ft(t,n,e=1/0){return we(n)?Ft((i,r)=>ee((o,s)=>n(i,o,r,s))(Qe(t(i,r))),e):(typeof n=="number"&&(e=n),ve((i,r)=>Mw(i,r,t,e)))}function Si(t=1/0){return Ft(Dn,t)}function Tw(){return Si(1)}function Ur(...t){return Tw()(Ge(t,Ei(t)))}function Zn(t){return new de(n=>{Qe(t()).subscribe(n)})}function fl(...t){let n=Hd(t),{args:e,keys:i}=iu(t),r=new de(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let u=0;u<s;u++){let f=!1;Qe(e[u]).subscribe(pe(o,h=>{f||(f=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?ru(i,a):a),o.complete())}))}});return n?r.pipe(nu(n)):r}function Aw(t=0,n,e=hw){let i=-1;return n!=null&&(Ud(n)?e=n:i=n),new de(r=>{let o=Iw(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Et(...t){let n=Ei(t),e=pw(t,1/0),i=t;return i.length?i.length===1?Qe(i[0]):Si(e)(Ge(i,n)):ut}function Ie(t,n){return ve((e,i)=>{let r=0;e.subscribe(pe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Rw(t){return ve((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(pe(e,c=>{i=!0,r=c,o||Qe(t(c)).subscribe(o=pe(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Bs(t,n=dl){return Rw(()=>Aw(t,n))}function Hr(t){return ve((n,e)=>{let i=null,r=!1,o;i=n.subscribe(pe(e,void 0,void 0,s=>{o=Qe(t(s,Hr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function $r(t,n){return we(n)?Ft(t,n,1):Ft(t,1)}function tr(t,n=dl){return ve((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}a()}e.subscribe(pe(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Ow(t){return ve((n,e)=>{let i=!1;n.subscribe(pe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function rt(t){return t<=0?()=>ut:ve((n,e)=>{let i=0;n.subscribe(pe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function ou(t){return ee(()=>t)}function su(t,n=Dn){return t=t??XA,ve((e,i)=>{let r,o=!0;e.subscribe(pe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function XA(t,n){return t===n}function Nw(t=JA){return ve((n,e)=>{let i=!1;n.subscribe(pe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function JA(){return new Oo}function No(t){return ve((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function nr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ie((r,o)=>t(r,o,i)):Dn,rt(1),e?Ow(n):Nw(()=>new Oo))}function au(t){return t<=0?()=>ut:ve((n,e)=>{let i=[];n.subscribe(pe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function lu(){return ve((t,n)=>{let e,i=!1;t.subscribe(pe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function ml(t={}){let{connector:n=()=>new I,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,u=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},m=()=>{h(),s=l=void 0,u=f=!1},_=()=>{let D=s;m(),D?.unsubscribe()};return ve((D,E)=>{c++,!f&&!u&&h();let M=l=l??n();E.add(()=>{c--,c===0&&!f&&!u&&(a=Np(_,r))}),M.subscribe(E),!s&&c>0&&(s=new er({next:Z=>M.next(Z),error:Z=>{f=!0,h(),a=Np(m,e,Z),M.error(Z)},complete:()=>{u=!0,h(),a=Np(m,i),M.complete()}}),Qe(D).subscribe(s))})(o)}}function Np(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new er({next:()=>{i.unsubscribe(),t()}});return Qe(n(...e)).subscribe(i)}function cu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,ml({connector:()=>new An(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function hl(t){return Ie((n,e)=>t<=e)}function St(...t){let n=Ei(t);return ve((e,i)=>{(n?Ur(t,e,n):Ur(t,e)).subscribe(i)})}function It(t,n){return ve((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(pe(i,l=>{r?.unsubscribe();let c=0,u=o++;Qe(t(l,u)).subscribe(r=pe(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function ue(t){return ve((n,e)=>{Qe(t).subscribe(pe(e,()=>e.complete(),ol)),!e.closed&&n.subscribe(e)})}function Fp(t,n=!1){return ve((e,i)=>{let r=0;e.subscribe(pe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function st(t,n,e){let i=we(t)||n||e?{next:t,error:n,complete:e}:t;return i?ve((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(pe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Dn}var Pp;function du(){return Pp}function Ii(t){let n=Pp;return Pp=t,n}var Fw=Symbol("NotFound");function Vs(t){return t===Fw||t?.name==="\u0275NotFound"}function Lp(t,n,e){let i=Object.create(eR);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(So(i),Lr(i),i.value===xi)throw i.error;return i.value};return o[dt]=i,tl(i),o}function Pw(t,n){So(t),Io(t,n),Ms(t)}function Lw(t,n){if(So(t),t.value===xi)throw t.error;Md(t,n),Ms(t)}var eR=oe(w({},Pr),{value:xo,dirty:!0,error:null,equal:nl,kind:"linkedSignal",producerMustRecompute(t){return t.value===xo||t.value===Eo},producerRecomputeValue(t){if(t.value===Eo)throw new Error("");let n=t.value;t.value=Eo;let e=Ji(t),i,r=!1;try{let o=t.source(),s=n!==xo&&n!==xi,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,ie(null),r=s&&i!==xi&&t.equal(n,i)}catch(o){i=xi,t.error=o}finally{Br(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Bw(t){let n=ie(null);try{return t()}finally{ie(n)}}var _u="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(n,e){super(Xn(n,e)),this.code=n}};function tR(t){return`NG0${Math.abs(t)}`}function Xn(t,n){return`${tR(t)}${n?": "+n:""}`}var Rn=globalThis;function je(t){for(let n in t)if(t[n]===je)return n;throw Error("")}function Hw(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function wl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(wl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function vu(t,n){return t?n?`${t} ${n}`:t:n||""}var nR=je({__forward_ref__:je});function kt(t){return t.__forward_ref__=kt,t}function _t(t){return Kp(t)?t():t}function Kp(t){return typeof t=="function"&&t.hasOwnProperty(nR)&&t.__forward_ref__===kt}function C(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function P(t){return{providers:t.providers||[],imports:t.imports||[]}}function Cl(t){return iR(t,yu)}function Zp(t){return Cl(t)!==null}function iR(t,n){return t.hasOwnProperty(n)&&t[n]||null}function rR(t){let n=t?.[yu]??null;return n||null}function Vp(t){return t&&t.hasOwnProperty(fu)?t[fu]:null}var yu=je({\u0275prov:je}),fu=je({\u0275inj:je}),b=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=C({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Xp(t){return t&&!!t.\u0275providers}var Jp=je({\u0275cmp:je}),eg=je({\u0275dir:je}),tg=je({\u0275pipe:je}),ng=je({\u0275mod:je}),gl=je({\u0275fac:je}),Vo=je({__NG_ELEMENT_ID__:je}),Vw=je({__NG_ENV_ID__:je});function ig(t){return wu(t,"@NgModule"),t[ng]||null}function Mi(t){return wu(t,"@Component"),t[Jp]||null}function bu(t){return wu(t,"@Directive"),t[eg]||null}function $w(t){return wu(t,"@Pipe"),t[tg]||null}function wu(t,n){if(t==null)throw new x(-919,!1)}function zs(t){return typeof t=="string"?t:t==null?"":String(t)}var Gw=je({ngErrorCode:je}),oR=je({ngErrorMessage:je}),sR=je({ngTokenPath:je});function rg(t,n){return Ww("",-200,n)}function Cu(t,n){throw new x(-201,!1)}function Ww(t,n,e){let i=new x(n,t);return i[Gw]=n,i[oR]=t,e&&(i[sR]=e),i}function aR(t){return t[Gw]}var jp;function qw(){return jp}function Jt(t){let n=jp;return jp=t,n}function og(t,n,e){let i=Cl(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Cu(t,"")}var lR={},Fo=lR,cR="__NG_DI_FLAG__",zp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Po(e)||0;try{return this.injector.get(n,i&8?null:Fo,i)}catch(r){if(Vs(r))return r;throw r}}};function dR(t,n=0){let e=du();if(e===void 0)throw new x(-203,!1);if(e===null)return og(t,void 0,n);{let i=uR(n),r=e.retrieve(t,i);if(Vs(r)){if(i.optional)return null;throw r}return r}}function V(t,n=0){return(qw()||dR)(_t(t),n)}function d(t,n){return V(t,Po(n))}function Po(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function uR(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Up(t){let n=[];for(let e=0;e<t.length;e++){let i=_t(t[e]);if(Array.isArray(i)){if(i.length===0)throw new x(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=fR(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(V(r,o))}else n.push(V(i))}return n}function fR(t){return t[cR]}function Gr(t,n){let e=t.hasOwnProperty(gl);return e?t[gl]:null}function Qw(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function Yw(t){return t.flat(Number.POSITIVE_INFINITY)}function Du(t,n){t.forEach(e=>Array.isArray(e)?Du(e,n):n(e))}function sg(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Dl(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Kw(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function Zw(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function xu(t,n,e){let i=Us(t,n);return i>=0?t[i|1]=e:(i=~i,Zw(t,i,n,e)),i}function Eu(t,n){let e=Us(t,n);if(e>=0)return t[e|1]}function Us(t,n){return mR(t,n,1)}function mR(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Jn={},zt=[],Qr=new b(""),ag=new b("",-1),lg=new b(""),_l=class{get(n,e=Fo){if(e===Fo){let r=Ww("",-201);throw r.name="\u0275NotFound",r}return e}};function Ti(t){return{\u0275providers:t}}function Xw(t){return Ti([{provide:Qr,multi:!0,useValue:t}])}function Jw(...t){return{\u0275providers:cg(!0,t),\u0275fromNgModule:!0}}function cg(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Du(n,s=>{let a=s;mu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&eC(r,o),e}function eC(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];dg(r,o=>{n(o,i)})}}function mu(t,n,e,i){if(t=_t(t),!t)return!1;let r=null,o=Vp(t),s=!o&&Mi(t);if(!o&&!s){let l=t.ngModule;if(o=Vp(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)mu(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;Du(o.imports,u=>{mu(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&eC(c,n)}if(!a){let c=Gr(r)||(()=>new r);n({provide:r,useFactory:c,deps:zt},r),n({provide:lg,useValue:r,multi:!0},r),n({provide:Qr,useValue:()=>V(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;dg(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function dg(t,n){for(let e of t)Xp(e)&&(e=e.\u0275providers),Array.isArray(e)?dg(e,n):n(e)}var hR=je({provide:String,useValue:je});function tC(t){return t!==null&&typeof t=="object"&&hR in t}function pR(t){return!!(t&&t.useExisting)}function gR(t){return!!(t&&t.useFactory)}function Lo(t){return typeof t=="function"}function nC(t){return!!t.useClass}var xl=new b(""),uu={},jw={},Bp;function Hs(){return Bp===void 0&&(Bp=new _l),Bp}var ze=class{},Bo=class extends ze{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,$p(n,s=>this.processProvider(s)),this.records.set(ag,js(void 0,this)),r.has("environment")&&this.records.set(ze,js(void 0,this));let o=this.records.get(xl);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(lg,zt,{self:!0}))}retrieve(n,e){let i=Po(e)||0;try{return this.get(n,Fo,i)}catch(r){if(Vs(r))return r;throw r}}destroy(){pl(this),this._destroyed=!0;let n=ie(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ie(n)}}onDestroy(n){return pl(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){pl(this);let e=Ii(this),i=Jt(void 0),r;try{return n()}finally{Ii(e),Jt(i)}}get(n,e=Fo,i){if(pl(this),n.hasOwnProperty(Vw))return n[Vw](this);let r=Po(i),o,s=Ii(this),a=Jt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=wR(n)&&Cl(n);u&&this.injectableDefInScope(u)?c=js(Hp(n),uu):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Hs():this.parent;return e=r&8&&e===Fo?null:e,l.get(n,e)}catch(l){let c=aR(l);throw c===-200||c===-201?new x(c,null):l}finally{Jt(a),Ii(s)}}resolveInjectorInitializers(){let n=ie(null),e=Ii(this),i=Jt(void 0),r;try{let o=this.get(Qr,zt,{self:!0});for(let s of o)s()}finally{Ii(e),Jt(i),ie(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=_t(n);let e=Lo(n)?n:_t(n&&n.provide),i=vR(n);if(!Lo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=js(void 0,uu,!0),r.factory=()=>Up(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ie(null);try{if(e.value===jw)throw rg("");return e.value===uu&&(e.value=jw,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&bR(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ie(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=_t(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Hp(t){let n=Cl(t),e=n!==null?n.factory:Gr(t);if(e!==null)return e;if(t instanceof b)throw new x(-204,!1);if(t instanceof Function)return _R(t);throw new x(-204,!1)}function _R(t){if(t.length>0)throw new x(-204,!1);let e=rR(t);return e!==null?()=>e.factory(t):()=>new t}function vR(t){if(tC(t))return js(void 0,t.useValue);{let n=ug(t);return js(n,uu)}}function ug(t,n,e){let i;if(Lo(t)){let r=_t(t);return Gr(r)||Hp(r)}else if(tC(t))i=()=>_t(t.useValue);else if(gR(t))i=()=>t.useFactory(...Up(t.deps||[]));else if(pR(t))i=(r,o)=>V(_t(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=_t(t&&(t.useClass||t.provide));if(yR(t))i=()=>new r(...Up(t.deps));else return Gr(r)||Hp(r)}return i}function pl(t){if(t.destroyed)throw new x(-205,!1)}function js(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function yR(t){return!!t.deps}function bR(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function wR(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function $p(t,n){for(let e of t)Array.isArray(e)?$p(e,n):e&&Xp(e)?$p(e.\u0275providers,n):n(e)}function yt(t,n){let e;t instanceof Bo?(pl(t),e=t):e=new zp(t);let i,r=Ii(e),o=Jt(void 0);try{return n()}finally{Ii(r),Jt(o)}}function iC(){return qw()!==void 0||du()!=null}var ei=0,re=1,fe=2,vt=3,On=4,tn=5,jo=6,$s=7,ft=8,rr=9,ti=10,Ye=11,Gs=12,fg=13,zo=14,nn=15,Yr=16,Uo=17,Ai=18,or=19,mg=20,ir=21,Su=22,Wr=23,xn=24,Ho=25,Kr=26,Je=27,rC=1,hg=6,Zr=7,El=8,$o=9,at=10;function sr(t){return Array.isArray(t)&&typeof t[rC]=="object"}function ni(t){return Array.isArray(t)&&t[rC]===!0}function pg(t){return(t.flags&4)!==0}function Ri(t){return t.componentOffset>-1}function Ws(t){return(t.flags&1)===1}function ii(t){return!!t.template}function qs(t){return(t[fe]&512)!==0}function Go(t){return(t[fe]&256)===256}var gg="svg",oC="math";function Nn(t){for(;Array.isArray(t);)t=t[ei];return t}function _g(t,n){return Nn(n[t])}function Fn(t,n){return Nn(n[t.index])}function Iu(t,n){return t.data[n]}function vg(t,n){return t[n]}function yg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Pn(t,n){let e=n[t];return sr(e)?e:e[ei]}function sC(t){return(t[fe]&4)===4}function ku(t){return(t[fe]&128)===128}function aC(t){return ni(t[vt])}function Ln(t,n){return n==null?null:t[n]}function bg(t){t[Uo]=0}function wg(t){t[fe]&1024||(t[fe]|=1024,ku(t)&&Wo(t))}function lC(t,n){for(;t>0;)n=n[zo],t--;return n}function Sl(t){return!!(t[fe]&9216||t[xn]?.dirty)}function Mu(t){t[ti].changeDetectionScheduler?.notify(8),t[fe]&64&&(t[fe]|=1024),Sl(t)&&Wo(t)}function Wo(t){t[ti].changeDetectionScheduler?.notify(0);let n=qr(t);for(;n!==null&&!(n[fe]&8192||(n[fe]|=8192,!ku(n)));)n=qr(n)}function Cg(t,n){if(Go(t))throw new x(911,!1);t[ir]===null&&(t[ir]=[]),t[ir].push(n)}function cC(t,n){if(t[ir]===null)return;let e=t[ir].indexOf(n);e!==-1&&t[ir].splice(e,1)}function qr(t){let n=t[vt];return ni(n)?n[vt]:n}function Dg(t){return t[$s]??=[]}function xg(t){return t.cleanup??=[]}function dC(t,n,e,i){let r=Dg(n);r.push(e),t.firstCreatePass&&xg(t).push(i,r.length-1)}var De={lFrame:CC(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Gp=!1;function uC(){return De.lFrame.elementDepthCount}function fC(){De.lFrame.elementDepthCount++}function Eg(){De.lFrame.elementDepthCount--}function Tu(){return De.bindingsEnabled}function Sg(){return De.skipHydrationRootTNode!==null}function Ig(t){return De.skipHydrationRootTNode===t}function kg(){De.skipHydrationRootTNode=null}function ce(){return De.lFrame.lView}function et(){return De.lFrame.tView}function Ae(t){return De.lFrame.contextLView=t,t[ft]}function Re(t){return De.lFrame.contextLView=null,t}function Mt(){let t=Mg();for(;t!==null&&t.type===64;)t=t.parent;return t}function Mg(){return De.lFrame.currentTNode}function mC(){let t=De.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Qs(t,n){let e=De.lFrame;e.currentTNode=t,e.isParent=n}function Tg(){return De.lFrame.isParent}function Ag(){De.lFrame.isParent=!1}function hC(){return De.lFrame.contextLView}function Rg(){return Gp}function vl(t){let n=Gp;return Gp=t,n}function Og(){let t=De.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function pC(){return De.lFrame.bindingIndex}function gC(t){return De.lFrame.bindingIndex=t}function Xr(){return De.lFrame.bindingIndex++}function Au(t){let n=De.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function _C(){return De.lFrame.inI18n}function vC(t,n){let e=De.lFrame;e.bindingIndex=e.bindingRootIndex=t,Ru(n)}function yC(){return De.lFrame.currentDirectiveIndex}function Ru(t){De.lFrame.currentDirectiveIndex=t}function bC(t){let n=De.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Ou(){return De.lFrame.currentQueryIndex}function Il(t){De.lFrame.currentQueryIndex=t}function CR(t){let n=t[re];return n.type===2?n.declTNode:n.type===1?t[tn]:null}function Ng(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=CR(o),r===null||(o=o[zo],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=De.lFrame=wC();return i.currentTNode=n,i.lView=t,!0}function Nu(t){let n=wC(),e=t[re];De.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function wC(){let t=De.lFrame,n=t===null?null:t.child;return n===null?CC(t):n}function CC(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function DC(){let t=De.lFrame;return De.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Fg=DC;function Fu(){let t=DC();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function xC(t){return(De.lFrame.contextLView=lC(t,De.lFrame.contextLView))[ft]}function Oi(){return De.lFrame.selectedIndex}function Jr(t){De.lFrame.selectedIndex=t}function kl(){let t=De.lFrame;return Iu(t.tView,t.selectedIndex)}function Ut(){De.lFrame.currentNamespace=gg}function qo(){DR()}function DR(){De.lFrame.currentNamespace=null}function Pg(){return De.lFrame.currentNamespace}var EC=!0;function Pu(){return EC}function Ml(t){EC=t}function Wp(t,n=null,e=null,i){let r=Lg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Lg(t,n=null,e=null,i,r=new Set){let o=[e||zt,Jw(t)],s;return new Bo(o,n||Hs(),s||null,r)}var Y=class t{static THROW_IF_NOT_FOUND=Fo;static NULL=new _l;static create(n,e){if(Array.isArray(n))return Wp({name:""},e,n,"");{let i=n.name??"";return Wp({name:i},n.parent,n.providers,i)}}static \u0275prov=C({token:t,providedIn:"any",factory:()=>V(ag)});static __NG_ELEMENT_ID__=-1},G=new b(""),Ht=(()=>{class t{static __NG_ELEMENT_ID__=xR;static __NG_ENV_ID__=e=>e}return t})(),hu=class extends Ht{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Go(this._lView)}onDestroy(n){let e=this._lView;return Cg(e,n),()=>cC(e,n)}};function xR(){return new hu(ce())}var SC=!1,IC=new b(""),ar=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new qe(!1);debugTaskTracker=d(IC,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new de(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),qp=class extends I{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,iC()&&(this.destroyRef=d(Ht,{optional:!0})??void 0,this.pendingTasks=d(ar,{optional:!0})??void 0)}emit(n){let e=ie(null);try{super.next(n)}finally{ie(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof Se&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Q=qp;function pu(...t){}function Bg(t){let n,e;function i(){t=pu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function kC(t){return queueMicrotask(()=>t()),()=>{t=pu}}var Vg="isAngularZone",yl=Vg+"_ID",ER=0,z=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Q(!1);onMicrotaskEmpty=new Q(!1);onStable=new Q(!1);onError=new Q(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=SC}=n;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,kR(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Vg)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new x(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,SR,pu,pu);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},SR={};function jg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function IR(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Bg(()=>{t.callbackScheduled=!1,Qp(t),t.isCheckStableRunning=!0,jg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Qp(t)}function kR(t){let n=()=>{IR(t)},e=ER++;t._inner=t._inner.fork({name:"angular",properties:{[Vg]:!0,[yl]:e,[yl+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(MR(l))return i.invokeTask(o,s,a,l);try{return zw(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Uw(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return zw(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!TR(l)&&n(),Uw(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Qp(t),jg(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Qp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function zw(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Uw(t){t._nesting--,jg(t)}var bl=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Q;onMicrotaskEmpty=new Q;onStable=new Q;onError=new Q;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function MR(t){return MC(t,"__ignore_ng_zone__")}function TR(t){return MC(t,"__scheduler_tick__")}function MC(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var en=class{_console=console;handleError(n){this._console.error("ERROR",n)}},En=new b("",{factory:()=>{let t=d(z),n=d(ze),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(en),e.handleError(i))})}}}),TC={provide:Qr,useValue:()=>{let t=d(en,{optional:!0})},multi:!0},AR=new b("",{factory:()=>{let t=d(G).defaultView;if(!t)return;let n=d(En),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(Ht).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function zg(){return Ti([Xw(()=>{d(AR)})])}function X(t,n){let[e,i,r]=yp(t,n?.equal),o=e,s=o[dt];return o.set=i,o.update=r,o.asReadonly=Lu.bind(o),o}function Lu(){let t=this[dt];if(t.readonlyFn===void 0){let n=()=>this();n[dt]=t,t.readonlyFn=n}return t.readonlyFn}var Ys=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=RR}return t})();function RR(){return new Ys(ce(),Mt())}var ki=class{},Tl=new b("",{factory:()=>!0});var Ug=new b(""),Al=(()=>{class t{internalPendingTasks=d(ar);scheduler=d(ki);errorHandler=d(En);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Bu=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>new Yp})}return t})(),Yp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},gu=class{[dt];constructor(n){this[dt]=n}destroy(){this[dt].destroy()}};function lr(t,n){let e=n?.injector??d(Y),i=n?.manualCleanup!==!0?e.get(Ht):null,r,o=e.get(Ys,null,{optional:!0}),s=e.get(ki);return o!==null?(r=FR(o.view,s,t),i instanceof hu&&i._lView===o.view&&(i=null)):r=PR(t,e.get(Bu),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new gu(r)}var AC=oe(w({},bp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=vl(!1);try{wp(this)}finally{vl(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ie(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ie(t)}}}),OR=oe(w({},AC),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Vr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),NR=oe(w({},AC),{consumerMarkedDirty(){this.view[fe]|=8192,Wo(this.view),this.notifier.notify(13)},destroy(){if(Vr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Wr]?.delete(this)}});function FR(t,n,e){let i=Object.create(NR);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=RC(i,e),t[Wr]??=new Set,t[Wr].add(i),i.consumerMarkedDirty(i),i}function PR(t,n,e){let i=Object.create(OR);return i.fn=RC(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function RC(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Hl(t){return{toString:t}.toString()}function HR(t){return typeof t=="function"}function pD(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Qu=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Pe=(()=>{let t=()=>gD;return t.ngInherit=!0,t})();function gD(t){return t.type.prototype.ngOnChanges&&(t.setInput=GR),$R}function $R(){let t=vD(this),n=t?.current;if(n){let e=t.previous;if(e===Jn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function GR(t,n,e,i,r){let o=this.declaredInputs[i],s=vD(t)||WR(t,{previous:Jn,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new Qu(c&&c.currentValue,e,l===Jn),pD(t,n,r,e)}var _D="__ngSimpleChanges__";function vD(t){return t[_D]||null}function WR(t,n){return t[_D]=n}var OC=[];var Ue=function(t,n=null,e){for(let i=0;i<OC.length;i++){let r=OC[i];r(t,n,e)}},Fe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Fe||{});function qR(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=gD(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function yD(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Hu(t,n,e){bD(t,n,3,e)}function $u(t,n,e,i){(t[fe]&3)===e&&bD(t,n,e,i)}function Hg(t,n){let e=t[fe];(e&3)===n&&(e&=16383,e+=1,t[fe]=e)}function bD(t,n,e,i){let r=i!==void 0?t[Uo]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[Uo]+=65536),(a<o||o==-1)&&(QR(t,e,n,l),t[Uo]=(t[Uo]&4294901760)+l+2),l++}function NC(t,n){Ue(Fe.LifecycleHookStart,t,n);let e=ie(null);try{n.call(t)}finally{ie(e),Ue(Fe.LifecycleHookEnd,t,n)}}function QR(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[fe]>>14<t[Uo]>>16&&(t[fe]&3)===n&&(t[fe]+=16384,NC(a,o)):NC(a,o)}var Zs=-1,Yo=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function YR(t){return(t.flags&8)!==0}function KR(t){return(t.flags&16)!==0}function ZR(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];XR(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function wD(t){return t===3||t===4||t===6}function XR(t){return t.charCodeAt(0)===64}function Xs(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?FC(t,e,r,null,n[++i]):FC(t,e,r,null,null))}}return t}function FC(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function CD(t){return t!==Zs}function Yu(t){return t&32767}function JR(t){return t>>16}function Ku(t,n){let e=JR(t),i=n;for(;e>0;)i=i[zo],e--;return i}var e_=!0;function Zu(t){let n=e_;return e_=t,n}var eO=256,DD=eO-1,xD=5,tO=0,Ni={};function nO(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Vo)&&(i=e[Vo]),i==null&&(i=e[Vo]=tO++);let r=i&DD,o=1<<r;n.data[t+(r>>xD)]|=o}function Xu(t,n){let e=ED(t,n);if(e!==-1)return e;let i=n[re];i.firstCreatePass&&(t.injectorIndex=n.length,$g(i.data,t),$g(n,null),$g(i.blueprint,null));let r=L_(t,n),o=t.injectorIndex;if(CD(r)){let s=Yu(r),a=Ku(r,n),l=a[re].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function $g(t,n){t.push(0,0,0,0,0,0,0,0,n)}function ED(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function L_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=TD(r),i===null)return Zs;if(e++,r=r[zo],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Zs}function t_(t,n,e){nO(t,n,e)}function iO(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(wD(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function SD(t,n,e){if(e&8||t!==void 0)return t;Cu(n,"NodeInjector")}function ID(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[rr],o=Jt(void 0);try{return r?r.get(n,i,e&8):og(n,i,e&8)}finally{Jt(o)}}return SD(i,n,e)}function kD(t,n,e,i=0,r){if(t!==null){if(n[fe]&2048&&!(i&2)){let s=aO(t,n,e,i,Ni);if(s!==Ni)return s}let o=MD(t,n,e,i,Ni);if(o!==Ni)return o}return ID(n,e,i,r)}function MD(t,n,e,i,r){let o=oO(e);if(typeof o=="function"){if(!Ng(n,t,i))return i&1?SD(r,e,i):ID(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Cu(e);else return s}finally{Fg()}}else if(typeof o=="number"){let s=null,a=ED(t,n),l=Zs,c=i&1?n[nn][tn]:null;for((a===-1||i&4)&&(l=a===-1?L_(t,n):n[a+8],l===Zs||!LC(i,!1)?a=-1:(s=n[re],a=Yu(l),n=Ku(l,n)));a!==-1;){let u=n[re];if(PC(o,a,u.data)){let f=rO(a,n,e,s,i,c);if(f!==Ni)return f}l=n[a+8],l!==Zs&&LC(i,n[re].data[a+8]===c)&&PC(o,a,n)?(s=u,a=Yu(l),n=Ku(l,n)):a=-1}}return r}function rO(t,n,e,i,r,o){let s=n[re],a=s.data[t+8],l=i==null?Ri(a)&&e_:i!=s&&(a.type&3)!==0,c=r&1&&o===a,u=Gu(a,s,e,l,c);return u!==null?Pl(n,s,u,a,r):Ni}function Gu(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?a:a+u,h=r?a+u:c;for(let m=f;m<h;m++){let _=s[m];if(m<l&&e===_||m>=l&&_.type===e)return m}if(r){let m=s[l];if(m&&ii(m)&&m.type===e)return l}return null}function Pl(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Yo){let a=o;if(a.resolving)throw rg("");let l=Zu(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],u,f=a.injectImpl?Jt(a.injectImpl):null,h=Ng(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&qR(e,s[e],n)}finally{f!==null&&Jt(f),Zu(l),a.resolving=!1,Fg()}}return o}function oO(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Vo)?t[Vo]:void 0;return typeof n=="number"?n>=0?n&DD:sO:n}function PC(t,n,e){let i=1<<t;return!!(e[n+(t>>xD)]&i)}function LC(t,n){return!(t&2)&&!(t&1&&n)}var Qo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return kD(this._tNode,this._lView,n,Po(i),e)}};function sO(){return new Qo(Mt(),ce())}function Me(t){return Hl(()=>{let n=t.prototype.constructor,e=n[gl]||n_(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[gl]||n_(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function n_(t){return Kp(t)?()=>{let n=n_(_t(t));return n&&n()}:Gr(t)}function aO(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[fe]&2048&&!qs(s);){let a=MD(o,s,e,i|2,Ni);if(a!==Ni)return a;let l=o.parent;if(!l){let c=s[mg];if(c){let u=c.get(e,Ni,i&-5);if(u!==Ni)return u}l=TD(s),s=s[zo]}o=l}return r}function TD(t){let n=t[re],e=n.type;return e===2?n.declTNode:e===1?t[tn]:null}function $l(t){return iO(Mt(),t)}function lO(){return na(Mt(),ce())}function na(t,n){return new O(Fn(t,n))}var O=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=lO}return t})();function AD(t){return t instanceof O?t.nativeElement:t}function cO(){return this._results[Symbol.iterator]()}var cr=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new I}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=Yw(n);(this._changesDetected=!Qw(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=cO};function RD(t){return(t.flags&128)===128}var B_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(B_||{}),OD=new Map,dO=0;function uO(){return dO++}function fO(t){OD.set(t[or],t)}function i_(t){OD.delete(t[or])}var BC="__ngContext__";function Js(t,n){sr(n)?(t[BC]=n[or],fO(n)):t[BC]=n}function ND(t){return PD(t[Gs])}function FD(t){return PD(t[On])}function PD(t){for(;t!==null&&!ni(t);)t=t[On];return t}var mO;function V_(t){mO=t}var no=new b("",{factory:()=>hO}),hO="ng";var ff=new b(""),Xo=new b("",{providedIn:"platform",factory:()=>"unknown"}),io=new b(""),Jo=new b("",{factory:()=>d(G).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var LD="r";var BD="di";var j_=new b(""),VD=!1,jD=new b("",{factory:()=>VD});var mf=new b("");var VC=new WeakMap;function pO(t,n){if(t==null||typeof t!="object")return;let e=VC.get(t);e||(e=new WeakSet,VC.set(t,e)),e.add(n)}var gO=(t,n,e,i)=>{};function _O(t,n,e,i){gO(t,n,e,i)}function hf(t){return(t.flags&32)===32}var vO=()=>null;function zD(t,n,e=!1){return vO(t,n,e)}function UD(t,n){let e=t.contentQueries;if(e!==null){let i=ie(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Il(o),a.contentQueries(2,n[s],s)}}}finally{ie(i)}}}function r_(t,n,e){Il(0);let i=ie(null);try{n(t,e)}finally{ie(i)}}function z_(t,n,e){if(pg(n)){let i=ie(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{ie(i)}}}var si=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(si||{});var Vu;function yO(){if(Vu===void 0&&(Vu=null,Rn.trustedTypes))try{Vu=Rn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Vu}function pf(t){return yO()?.createHTML(t)||t}var ju;function bO(){if(ju===void 0&&(ju=null,Rn.trustedTypes))try{ju=Rn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return ju}function jC(t){return bO()?.createScriptURL(t)||t}var dr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${_u})`}},o_=class extends dr{getTypeName(){return"HTML"}},s_=class extends dr{getTypeName(){return"Style"}},a_=class extends dr{getTypeName(){return"Script"}},l_=class extends dr{getTypeName(){return"URL"}},c_=class extends dr{getTypeName(){return"ResourceURL"}};function Bn(t){return t instanceof dr?t.changingThisBreaksApplicationSecurity:t}function ur(t,n){let e=HD(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${_u})`)}return e===n}function HD(t){return t instanceof dr&&t.getTypeName()||null}function U_(t){return new o_(t)}function H_(t){return new s_(t)}function $_(t){return new a_(t)}function G_(t){return new l_(t)}function W_(t){return new c_(t)}function wO(t){let n=new u_(t);return CO()?new d_(n):n}var d_=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(pf(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},u_=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=pf(n),e}};function CO(){try{return!!new window.DOMParser().parseFromString(pf(""),"text/html")}catch{return!1}}var DO=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Gl(t){return t=String(t),t.match(DO)?t:"unsafe:"+t}function fr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Wl(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var $D=fr("area,br,col,hr,img,wbr"),GD=fr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),WD=fr("rp,rt"),xO=Wl(WD,GD),EO=Wl(GD,fr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),SO=Wl(WD,fr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),zC=Wl($D,EO,SO,xO),qD=fr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),IO=fr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),kO=fr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),MO=Wl(qD,IO,kO),TO=fr("script,style,template"),f_=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=OO(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=RO(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=UC(n).toLowerCase();if(!zC.hasOwnProperty(e))return this.sanitizedSomething=!0,!TO.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!MO.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let l=o.value;qD[a]&&(l=Gl(l)),this.buf.push(" ",s,'="',HC(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=UC(n).toLowerCase();zC.hasOwnProperty(e)&&!$D.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(HC(n))}};function AO(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function RO(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw QD(n);return n}function OO(t){let n=t.firstChild;if(n&&AO(t,n))throw QD(n);return n}function UC(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function QD(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var NO=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,FO=/([^\#-~ |!])/g;function HC(t){return t.replace(/&/g,"&amp;").replace(NO,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(FO,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var zu;function q_(t,n){let e=null;try{zu=zu||wO(t);let i=n?String(n):"";e=zu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=zu.getInertBodyElement(i)}while(i!==o);let a=new f_().sanitizeChildren($C(e)||e);return pf(a)}finally{if(e){let i=$C(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function $C(t){return"content"in t&&PO(t)?t.content:null}function PO(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var LO=/^>|^->|<!--|-->|--!>|<!-$/g,BO=/(<|>)/g,VO="\u200B$1\u200B";function jO(t){return t.replace(LO,n=>n.replace(BO,VO))}function zO(t,n){return t.createText(n)}function UO(t,n,e){t.setValue(n,e)}function HO(t,n){return t.createComment(jO(n))}function YD(t,n,e){return t.createElement(n,e)}function Ju(t,n,e,i,r){t.insertBefore(n,e,i,r)}function KD(t,n,e){t.appendChild(n,e)}function GC(t,n,e,i,r){i!==null?Ju(t,n,e,i,r):KD(t,n,e)}function ZD(t,n,e,i){t.removeChild(null,n,e,i)}function $O(t,n,e){t.setAttribute(n,"style",e)}function GO(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function XD(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&ZR(t,n,i),r!==null&&GO(t,n,r),o!==null&&$O(t,n,o)}var bt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(bt||{});function ql(t){let n=ex();return n?n.sanitize(bt.URL,t)||"":ur(t,"URL")?Bn(t):Gl(zs(t))}function JD(t){let n=ex();if(n)return jC(n.sanitize(bt.RESOURCE_URL,t)||"");if(ur(t,"ResourceURL"))return jC(Bn(t));throw new x(904,!1)}var WO={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function qO(t,n){return WO[t.toLowerCase()]?.[n.toLowerCase()]===!0?JD:ql}function Q_(t,n,e){return qO(n,e)(t)}function ex(){let t=ce();return t&&t[ti].sanitizer}function tx(t){return t instanceof Function?t():t}function QO(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var nx="ng-template";function YO(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&QO(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Y_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Y_(t){return t.type===4&&t.value!==nx}function KO(t,n,e){let i=t.type===4&&!e?nx:t.value;return n===i}function ZO(t,n,e){let i=4,r=t.attrs,o=r!==null?eN(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!ri(i)&&!ri(l))return!1;if(s&&ri(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!KO(t,l,e)||l===""&&n.length===1){if(ri(i))return!1;s=!0}}else if(i&8){if(r===null||!YO(t,r,l,e)){if(ri(i))return!1;s=!0}}else{let c=n[++a],u=XO(l,r,Y_(t),e);if(u===-1){if(ri(i))return!1;s=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(ri(i))return!1;s=!0}}}}return ri(i)||s}function ri(t){return(t&1)===0}function XO(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return tN(n,t)}function ix(t,n,e=!1){for(let i=0;i<n.length;i++)if(ZO(t,n[i],e))return!0;return!1}function JO(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function eN(t){for(let n=0;n<t.length;n++){let e=t[n];if(wD(e))return n}return t.length}function tN(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function nN(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function WC(t,n){return t?":not("+n.trim()+")":n}function iN(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!ri(s)&&(n+=WC(o,r),r=""),i=s,o=o||!ri(i);e++}return r!==""&&(n+=WC(o,r)),n}function rN(t){return t.map(iN).join(",")}function oN(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!ri(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var rn={};function K_(t,n,e,i,r,o,s,a,l,c,u){let f=Je+i,h=f+r,m=sN(f,h),_=typeof c=="function"?c():c;return m[re]={type:t,blueprint:m,template:e,queries:null,viewQuery:a,declTNode:n,data:m.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:_,incompleteFirstPass:!1,ssrId:u}}function sN(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:rn);return e}function aN(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=K_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Z_(t,n,e,i,r,o,s,a,l,c,u){let f=n.blueprint.slice();return f[ei]=r,f[fe]=i|4|128|8|64|1024,(c!==null||t&&t[fe]&2048)&&(f[fe]|=2048),bg(f),f[vt]=f[zo]=t,f[ft]=e,f[ti]=s||t&&t[ti],f[Ye]=a||t&&t[Ye],f[rr]=l||t&&t[rr]||null,f[tn]=o,f[or]=uO(),f[jo]=u,f[mg]=c,f[nn]=n.type==2?t[nn]:f,f}function lN(t,n,e){let i=Fn(n,t),r=aN(e),o=t[ti].rendererFactory,s=X_(t,Z_(t,r,null,rx(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function rx(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function ox(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function X_(t,n){return t[Gs]?t[fg][On]=n:t[Gs]=n,t[fg]=n,n}function v(t=1){sx(et(),ce(),Oi()+t,!1)}function sx(t,n,e,i){if(!i)if((n[fe]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Hu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&$u(n,o,0,e)}Jr(e)}var gf=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(gf||{});function m_(t,n,e,i){let r=ie(null);try{let[o,s,a]=t.inputs[e],l=null;(s&gf.SignalBased)!==0&&(l=n[o][dt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):pD(n,l,o,i)}finally{ie(r)}}var Fi=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Fi||{}),cN;function J_(t,n){return cN(t,n)}var D9=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var h_=new WeakMap,Ol=new WeakSet;function dN(t,n){let e=h_.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===n?(e.splice(o,1),Ol.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function uN(t,n){let e=h_.get(t);e?e.includes(n)||e.push(n):h_.set(t,[n])}var Ko=new Set,_f=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(_f||{}),li=new b(""),qC=new Set;function Vn(t){qC.has(t)||(qC.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var vf=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),ev=[0,1,2,3],tv=(()=>{class t{ngZone=d(z);scheduler=d(ki);errorHandler=d(en,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(li,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Ue(Fe.AfterRenderHooksStart),this.executing=!0;for(let i of ev)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Ue(Fe.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Ho]??=[]).push(e),Wo(i),i[fe]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(_f.AFTER_NEXT_RENDER,e):e()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ll=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Ho];n&&(this.view[Ho]=n.filter(e=>e!==this))}};function Ke(t,n){let e=n?.injector??d(Y);return Vn("NgAfterNextRender"),mN(t,e,n,!0)}function fN(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function mN(t,n,e,i){let r=n.get(vf);r.impl??=n.get(tv);let o=n.get(li,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(Ht):null,a=n.get(Ys,null,{optional:!0}),l=new Ll(r.impl,fN(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var ax=new b("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(ze)})});function lx(t,n,e){let i=t.get(ax);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function hN(t,n){let e=t.get(ax);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function pN(t,n){for(let[e,i]of n)lx(t,i.animateFns)}function QC(t,n,e,i){let r=t?.[Kr]?.enter;n!==null&&r&&r.has(e.index)&&pN(i,r)}function Ks(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;ni(r)?l=r:sr(r)&&(c=!0,r=r[ei]);let u=Nn(r);t===0&&i!==null?(QC(a,i,o,e),s==null?KD(n,i,u):Ju(n,i,u,s||null,!0)):t===1&&i!==null?(QC(a,i,o,e),Ju(n,i,u,s||null,!0),dN(o,u)):t===2?(a?.[Kr]?.leave?.has(o.index)&&uN(o,u),Ol.delete(u),YC(a,o,e,f=>{if(Ol.has(u)){Ol.delete(u);return}ZD(n,u,c,f)})):t===3&&(Ol.delete(u),YC(a,o,e,()=>{n.destroyNode(u)})),l!=null&&SN(n,t,e,l,o,i,s)}}function gN(t,n){cx(t,n),n[ei]=null,n[tn]=null}function _N(t,n,e,i,r,o){i[ei]=r,i[tn]=n,bf(t,i,e,1,r,o)}function cx(t,n){n[ti].changeDetectionScheduler?.notify(9),bf(t,n,n[Ye],2,null,null)}function vN(t){let n=t[Gs];if(!n)return Gg(t[re],t);for(;n;){let e=null;if(sr(n))e=n[Gs];else{let i=n[at];i&&(e=i)}if(!e){for(;n&&!n[On]&&n!==t;)sr(n)&&Gg(n[re],n),n=n[vt];n===null&&(n=t),sr(n)&&Gg(n[re],n),e=n&&n[On]}n=e}}function nv(t,n){let e=t[$o],i=e.indexOf(n);e.splice(i,1)}function yf(t,n){if(Go(n))return;let e=n[Ye];e.destroyNode&&bf(t,n,e,3,null,null),vN(n)}function Gg(t,n){if(Go(n))return;let e=ie(null);try{n[fe]&=-129,n[fe]|=256,n[xn]&&Vr(n[xn]),wN(t,n),bN(t,n),n[re].type===1&&n[Ye].destroy();let i=n[Yr];if(i!==null&&ni(n[vt])){i!==n[vt]&&nv(i,n);let r=n[Ai];r!==null&&r.detachView(t)}i_(n)}finally{ie(e)}}function YC(t,n,e,i){let r=t?.[Kr];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Ko.add(t[or]),lx(e,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let l=0;l<s.animateFns.length;l++){let c=s.animateFns[l],{promise:u}=c();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),yN(t,i)}else t&&Ko.delete(t[or]),i(!1)},r)}function yN(t,n){let e=t[Kr]?.running;if(e){e.then(()=>{t[Kr].running=void 0,Ko.delete(t[or]),n(!0)});return}n(!1)}function bN(t,n){let e=t.cleanup,i=n[$s];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[$s]=null);let r=n[ir];if(r!==null){n[ir]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Wr];if(o!==null){n[Wr]=null;for(let s of o)s.destroy()}}function wN(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Yo)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];Ue(Fe.LifecycleHookStart,a,l);try{l.call(a)}finally{Ue(Fe.LifecycleHookEnd,a,l)}}else{Ue(Fe.LifecycleHookStart,r,o);try{o.call(r)}finally{Ue(Fe.LifecycleHookEnd,r,o)}}}}}function dx(t,n,e){return CN(t,n.parent,e)}function CN(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[ei];if(Ri(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===si.None||r===si.Emulated)return null}return Fn(i,e)}function ux(t,n,e){return xN(t,n,e)}function DN(t,n,e){return t.type&40?Fn(t,e):null}var xN=DN,KC;function iv(t,n,e,i){let r=dx(t,i,n),o=n[Ye],s=i.parent||n[tn],a=ux(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)GC(o,r,e[l],a,!1);else GC(o,r,e,a,!1);KC!==void 0&&KC(o,i,n,e,r)}function Nl(t,n){if(n!==null){let e=n.type;if(e&3)return Fn(n,t);if(e&4)return p_(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Nl(t,i);{let r=t[n.index];return ni(r)?p_(-1,r):Nn(r)}}else{if(e&128)return Nl(t,n.next);if(e&32)return J_(n,t)()||Nn(t[n.index]);{let i=fx(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=qr(t[nn]);return Nl(r,i)}else return Nl(t,n.next)}}}return null}function fx(t,n){if(n!==null){let i=t[nn][tn],r=n.projection;return i.projection[r]}return null}function p_(t,n){let e=at+t+1;if(e<n.length){let i=n[e],r=i[re].firstChild;if(r!==null)return Nl(i,r)}return n[Zr]}function rv(t,n,e,i,r,o,s){for(;e!=null;){let a=i[rr];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&Js(Nn(l),i),e.flags|=2),!hf(e))if(c&8)rv(t,n,e.child,i,r,o,!1),Ks(n,t,a,r,l,e,o,i);else if(c&32){let u=J_(e,i),f;for(;f=u();)Ks(n,t,a,r,f,e,o,i);Ks(n,t,a,r,l,e,o,i)}else c&16?mx(t,n,i,e,r,o):Ks(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function bf(t,n,e,i,r,o){rv(e,i,t.firstChild,n,r,o,!1)}function EN(t,n,e){let i=n[Ye],r=dx(t,e,n),o=e.parent||n[tn],s=ux(o,e,n);mx(i,0,n,e,r,s)}function mx(t,n,e,i,r,o){let s=e[nn],l=s[tn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Ks(n,t,e[rr],r,u,i,o,e)}else{let c=l,u=s[vt];RD(i)&&(c.flags|=128),rv(t,n,c,u,r,o,!0)}}function SN(t,n,e,i,r,o,s){let a=i[Zr],l=Nn(i);a!==l&&Ks(n,t,e,o,a,r,s);for(let c=at;c<i.length;c++){let u=i[c];bf(u[re],u,t,n,o,a)}}function IN(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Fi.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Fi.Important),t.setStyle(e,i,r,o))}}function hx(t,n,e,i,r){let o=Oi(),s=i&2;try{Jr(-1),s&&n.length>Je&&sx(t,n,Je,!1);let a=s?Fe.TemplateUpdateStart:Fe.TemplateCreateStart;Ue(a,r,e),e(i,r)}finally{Jr(o);let a=s?Fe.TemplateUpdateEnd:Fe.TemplateCreateEnd;Ue(a,r,e)}}function wf(t,n,e){ON(t,n,e),(e.flags&64)===64&&NN(t,n,e)}function Ql(t,n,e=Fn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function kN(t,n,e,i){let o=i.get(jD,VD)||e===si.ShadowDom||e===si.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new x(905,!1);return MN(s),s}function MN(t){TN(t)}var TN=()=>null;function AN(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function RN(t,n,e,i,r,o){let s=n[re];if(Cf(t,s,n,e,i)){Ri(t)&&gx(n,t.index);return}t.type&3&&(e=AN(e)),px(t,n,e,i,r,o)}function px(t,n,e,i,r,o){if(t.type&3){let s=Fn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function gx(t,n){let e=Pn(n,t);e[fe]&16||(e[fe]|=64)}function ON(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Ri(e)&&lN(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Xu(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=Pl(n,t,s,e);if(Js(l,n),o!==null&&LN(n,s-i,l,a,e,o),ii(a)){let c=Pn(e.index,n);c[ft]=Pl(n,t,s,e)}}}function NN(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=yC();try{Jr(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];Ru(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&FN(l,c)}}finally{Jr(-1),Ru(s)}}function FN(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function ov(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];ix(n,o.selectors,!1)&&(i??=[],ii(o)?i.unshift(o):i.push(o))}return i}function PN(t,n,e,i,r,o){let s=Fn(t,n);_x(n[Ye],s,o,t.value,e,i,r)}function _x(t,n,e,i,r,o,s){if(o==null)t.removeAttribute(n,r,e);else{let a=s==null?zs(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function LN(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];m_(i,e,l,c)}}function sv(t,n,e,i,r){let o=Je+e,s=n[re],a=r(s,n,t,i,e);n[o]=a,Qs(t,!0);let l=t.type===2;return l?(XD(n[Ye],a,t),(uC()===0||Ws(t))&&Js(a,n),fC()):Js(a,n),Pu()&&(!l||!hf(t))&&iv(s,n,a,t),t}function av(t){let n=t;return Tg()?Ag():(n=n.parent,Qs(n,!1)),n}function BN(t,n){let e=t[rr];if(!e)return;let i;try{i=e.get(En,null)}catch{i=null}i?.(n)}function Cf(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],u=s[l+1],f=n.data[c];m_(f,e[c],u,r),a=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];m_(u,c,i,r),a=!0}return a}function VN(t,n){let e=Pn(n,t),i=e[re];jN(i,e);let r=e[ei];r!==null&&e[jo]===null&&(e[jo]=zD(r,e[rr])),Ue(Fe.ComponentStart);try{lv(i,e,e[ft])}finally{Ue(Fe.ComponentEnd,e[ft])}}function jN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function lv(t,n,e){Nu(n);try{let i=t.viewQuery;i!==null&&r_(1,i,e);let r=t.template;r!==null&&hx(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Ai]?.finishViewCreation(t),t.staticContentQueries&&UD(t,n),t.staticViewQueries&&r_(2,t.viewQuery,e);let o=t.components;o!==null&&zN(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[fe]&=-5,Fu()}}function zN(t,n){for(let e=0;e<n.length;e++)VN(t,n[e])}function Yl(t,n,e,i){let r=ie(null);try{let o=n.tView,a=t[fe]&4096?4096:16,l=Z_(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Yr]=c;let u=t[Ai];return u!==null&&(l[Ai]=u.createEmbeddedView(o)),lv(o,l,e),l}finally{ie(r)}}function ea(t,n){return!n||n.firstChild===null||RD(t)}function Bl(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(Nn(o)),ni(o)&&vx(o,i);let s=e.type;if(s&8)Bl(t,n,e.child,i);else if(s&32){let a=J_(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=fx(n,e);if(Array.isArray(a))i.push(...a);else{let l=qr(n[nn]);Bl(l[re],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function vx(t,n){for(let e=at;e<t.length;e++){let i=t[e],r=i[re].firstChild;r!==null&&Bl(i[re],i,r,n)}t[Zr]!==t[ei]&&n.push(t[Zr])}function yx(t){if(t[Ho]!==null){for(let n of t[Ho])n.impl.addSequence(n);t[Ho].length=0}}var bx=[];function UN(t){return t[xn]??HN(t)}function HN(t){let n=bx.pop()??Object.create(GN);return n.lView=t,n}function $N(t){t.lView[xn]!==t&&(t.lView=null,bx.push(t))}var GN=oe(w({},Pr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Wo(t.lView)},consumerOnSignalRead(){this.lView[xn]=this}});function WN(t){let n=t[xn]??Object.create(qN);return n.lView=t,n}var qN=oe(w({},Pr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=qr(t.lView);for(;n&&!wx(n[re]);)n=qr(n);n&&wg(n)},consumerOnSignalRead(){this.lView[xn]=this}});function wx(t){return t.type!==2}function Cx(t){if(t[Wr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Wr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[fe]&8192)}}var QN=100;function Dx(t,n=0){let i=t[ti].rendererFactory,r=!1;r||i.begin?.();try{YN(t,n)}finally{r||i.end?.()}}function YN(t,n){let e=Rg();try{vl(!0),g_(t,n);let i=0;for(;Sl(t);){if(i===QN)throw new x(103,!1);i++,g_(t,1)}}finally{vl(e)}}function KN(t,n,e,i){if(Go(n))return;let r=n[fe],o=!1,s=!1;Nu(n);let a=!0,l=null,c=null;o||(wx(t)?(c=UN(n),l=Ji(c)):kd()===null?(a=!1,c=WN(n),l=Ji(c)):n[xn]&&(Vr(n[xn]),n[xn]=null));try{bg(n),gC(t.bindingStartIndex),e!==null&&hx(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let m=t.preOrderCheckHooks;m!==null&&Hu(n,m,null)}else{let m=t.preOrderHooks;m!==null&&$u(n,m,0,null),Hg(n,0)}if(s||ZN(n),Cx(n),xx(n,0),t.contentQueries!==null&&UD(t,n),!o)if(u){let m=t.contentCheckHooks;m!==null&&Hu(n,m)}else{let m=t.contentHooks;m!==null&&$u(n,m,1),Hg(n,1)}JN(t,n);let f=t.components;f!==null&&Sx(n,f,0);let h=t.viewQuery;if(h!==null&&r_(2,h,i),!o)if(u){let m=t.viewCheckHooks;m!==null&&Hu(n,m)}else{let m=t.viewHooks;m!==null&&$u(n,m,2),Hg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Su]){for(let m of n[Su])m();n[Su]=null}o||(yx(n),n[fe]&=-73)}catch(u){throw o||Wo(n),u}finally{c!==null&&(Br(c,l),a&&$N(c)),Fu()}}function xx(t,n){for(let e=ND(t);e!==null;e=FD(e))for(let i=at;i<e.length;i++){let r=e[i];Ex(r,n)}}function ZN(t){for(let n=ND(t);n!==null;n=FD(n)){if(!(n[fe]&2))continue;let e=n[$o];for(let i=0;i<e.length;i++){let r=e[i];wg(r)}}}function XN(t,n,e){Ue(Fe.ComponentStart);let i=Pn(n,t);try{Ex(i,e)}finally{Ue(Fe.ComponentEnd,i[ft])}}function Ex(t,n){ku(t)&&g_(t,n)}function g_(t,n){let i=t[re],r=t[fe],o=t[xn],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Ts(o)),s||=!1,o&&(o.dirty=!1),t[fe]&=-9217,s)KN(i,t,i.template,t[ft]);else if(r&8192){let a=ie(null);try{Cx(t),xx(t,1);let l=i.components;l!==null&&Sx(t,l,1),yx(t)}finally{ie(a)}}}function Sx(t,n,e){for(let i=0;i<n.length;i++)XN(t,n[i],e)}function JN(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Jr(~r);else{let o=r,s=e[++i],a=e[++i];vC(s,o);let l=n[o];Ue(Fe.HostBindingsUpdateStart,l);try{a(2,l)}finally{Ue(Fe.HostBindingsUpdateEnd,l)}}}}finally{Jr(-1)}}function cv(t,n){let e=Rg()?64:1088;for(t[ti].changeDetectionScheduler?.notify(n);t;){t[fe]|=e;let i=qr(t);if(qs(t)&&!i)return t;t=i}return null}function Ix(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function kx(t,n){let e=at+n;if(e<t.length)return t[e]}function Kl(t,n,e,i=!0){let r=n[re];if(e1(r,n,t,e),i){let s=p_(e,t),a=n[Ye],l=a.parentNode(t[Zr]);l!==null&&_N(r,t[tn],a,n,l,s)}let o=n[jo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Mx(t,n){let e=Vl(t,n);return e!==void 0&&yf(e[re],e),e}function Vl(t,n){if(t.length<=at)return;let e=at+n,i=t[e];if(i){let r=i[Yr];r!==null&&r!==t&&nv(r,i),n>0&&(t[e-1][On]=i[On]);let o=Dl(t,at+n);gN(i[re],i);let s=o[Ai];s!==null&&s.detachView(o[re]),i[vt]=null,i[On]=null,i[fe]&=-129}return i}function e1(t,n,e,i){let r=at+i,o=e.length;i>0&&(e[r-1][On]=n),i<o-at?(n[On]=e[r],sg(e,at+i,n)):(e.push(n),n[On]=null),n[vt]=e;let s=n[Yr];s!==null&&e!==s&&Tx(s,n);let a=n[Ai];a!==null&&a.insertView(t),Mu(n),n[fe]|=128}function Tx(t,n){let e=t[$o],i=n[vt];if(sr(i))t[fe]|=2;else{let r=i[vt][nn];n[nn]!==r&&(t[fe]|=2)}e===null?t[$o]=[n]:e.push(n)}var eo=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[re];return Bl(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[ft]}set context(n){this._lView[ft]=n}get destroyed(){return Go(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[vt];if(ni(n)){let e=n[El],i=e?e.indexOf(this):-1;i>-1&&(Vl(n,i),Dl(e,i))}this._attachedToViewContainer=!1}yf(this._lView[re],this._lView)}onDestroy(n){Cg(this._lView,n)}markForCheck(){cv(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[fe]&=-129}reattach(){Mu(this._lView),this._lView[fe]|=128}detectChanges(){this._lView[fe]|=1024,Dx(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=qs(this._lView),e=this._lView[Yr];e!==null&&!n&&nv(e,this._lView),cx(this._lView[re],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=n;let e=qs(this._lView),i=this._lView[Yr];i!==null&&!e&&Tx(i,this._lView),Mu(this._lView)}};var Xe=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=t1;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Yl(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new eo(o)}}return t})();function t1(){return Df(Mt(),ce())}function Df(t,n){return t.type&4?new Xe(n,t,na(t,n)):null}function ia(t,n,e,i,r){let o=t.data[n];if(o===null)o=n1(t,n,e,i,r),_C()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=mC();o.injectorIndex=s===null?-1:s.injectorIndex}return Qs(o,!0),o}function n1(t,n,e,i,r){let o=Mg(),s=Tg(),a=s?o:o&&o.parent,l=t.data[n]=r1(t,a,e,n,i,r);return i1(t,l,o,s),l}function i1(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function r1(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Sg()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Pg(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function o1(t){let n=t[hg]??[],i=t[vt][Ye],r=[];for(let o of n)o.data[BD]!==void 0?r.push(o):s1(o,i);t[hg]=r}function s1(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[LD];for(;e<r;){let o=i.nextSibling;ZD(n,i,!1),i=o,e++}}}var a1=()=>null,l1=()=>null;function ef(t,n){return a1(t,n)}function Ax(t,n,e){return l1(t,n,e)}var Rx=class{},xf=class{},__=class{resolveComponentFactory(n){throw new x(917,!1)}},Zl=class{static NULL=new __},lt=class{},$e=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>c1()}return t})();function c1(){let t=ce(),n=Mt(),e=Pn(n.index,t);return(sr(e)?e:t)[Ye]}var Ox=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>null})}return t})();var Wu={},v_=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Wu,i);return r!==Wu||e===Wu?r:this.parentInjector.get(n,e,i)}};function tf(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=vu(r,a);else if(o==2){let l=a,c=n[++s];i=vu(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function R(t,n=0){let e=ce();if(e===null)return V(t,n);let i=Mt();return kD(i,e,_t(t),n)}function dv(){let t="invalid";throw new Error(t)}function Nx(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,l,c]=u.resolveHostDirectives(s);break}f1(t,n,e,a,o,l,c)}o!==null&&i!==null&&d1(e,i,o)}function d1(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new x(-301,!1);i.push(n[r],o)}}function u1(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function f1(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let m=i[h];l===null&&ii(m)&&(l=m,u1(t,e,h)),t_(Xu(e,n),t,m.type)}v1(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let m=i[h];m.providersResolver&&m.providersResolver(m)}let c=!1,u=!1,f=ox(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let m=i[h];if(e.mergedAttrs=Xs(e.mergedAttrs,m.hostAttrs),h1(t,e,n,f,m),_1(f,m,r),s!==null&&s.has(m)){let[D,E]=s.get(m);e.directiveToIndex.set(m.type,[f,D+e.directiveStart,E+e.directiveStart])}else(o===null||!o.has(m))&&e.directiveToIndex.set(m.type,f);m.contentQueries!==null&&(e.flags|=4),(m.hostBindings!==null||m.hostAttrs!==null||m.hostVars!==0)&&(e.flags|=64);let _=m.type.prototype;!c&&(_.ngOnChanges||_.ngOnInit||_.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(_.ngOnChanges||_.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}m1(t,e,o)}function m1(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))ZC(0,n,r,i),ZC(1,n,r,i),JC(n,i,!1);else{let o=e.get(r);XC(0,n,o,i),XC(1,n,o,i),JC(n,i,!0)}}}function ZC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),Fx(n,o)}}function XC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Fx(n,s)}}function Fx(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function JC(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Y_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){s??=[],s.push(c[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function h1(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Gr(r.type,!0)),s=new Yo(o,ii(r),R,null);t.blueprint[i]=s,e[i]=s,p1(t,n,i,ox(t,e,r.hostVars,rn),r)}function p1(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;g1(s)!=a&&s.push(a),s.push(e,i,o)}}function g1(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function _1(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;ii(n)&&(e[""]=t)}}function v1(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function uv(t,n,e,i,r,o,s,a){let l=n[re],c=l.consts,u=Ln(c,s),f=ia(l,t,e,i,u);return o&&Nx(l,n,f,Ln(c,a),r),f.mergedAttrs=Xs(f.mergedAttrs,f.attrs),f.attrs!==null&&tf(f,f.attrs,!1),f.mergedAttrs!==null&&tf(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function fv(t,n){yD(t,n),pg(n)&&t.queries.elementEnd(n)}function y1(t,n,e,i,r,o){let s=n.consts,a=Ln(s,r),l=ia(n,t,e,i,a);if(l.mergedAttrs=Xs(l.mergedAttrs,l.attrs),o!=null){let c=Ln(s,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&tf(l,l.attrs,!1),l.mergedAttrs!==null&&tf(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function mv(t){return Lx(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function Px(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function Lx(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function Bx(t,n,e){return t[n]=e}function b1(t,n){return t[n]}function ai(t,n,e){if(e===rn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Vx(t,n,e,i){let r=ai(t,n,e);return ai(t,n+1,i)||r}function qu(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&pO(r,o);let s=Ri(t)?Pn(t.index,n):n;cv(s,5);let a=n[ft],l=eD(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=eD(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function eD(t,n,e,i){let r=ie(null);try{return Ue(Fe.OutputStart,n,e),e(i)!==!1}catch(o){return BN(t,o),!1}finally{Ue(Fe.OutputEnd,n,e),ie(r)}}function jx(t,n,e,i,r,o,s,a){let l=Ws(t),c=!1,u=null;if(!i&&l&&(u=C1(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,c=!0}else{let f=Fn(t,e),h=i?i(f):f;_O(e,h,o,a),i||(a.__ngNativeEl__=f);let m=r.listen(h,o,a);if(!w1(o)){let _=i?D=>i(Nn(D[t.index])):t.index;zx(_,n,e,o,a,m,!1)}}return c}function w1(t){return t.startsWith("animation")||t.startsWith("transition")}function C1(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[$s],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function zx(t,n,e,i,r,o,s){let a=n.firstCreatePass?xg(n):null,l=Dg(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function tD(t,n,e,i,r,o){let s=n[e],a=n[re],c=a.data[e].outputs[i],f=s[c].subscribe(o);zx(t.index,a,n,r,o,f,!0)}var y_=Symbol("BINDING");function Ux(t){return t.debugInfo?.className||t.type.name||null}var nf=class extends Zl{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=Mi(n);return new to(e,this.ngModule)}};function D1(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&gf.SignalBased)!==0};return r&&(o.transform=r),o})}function x1(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function E1(t,n,e){let i=n instanceof ze?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new v_(e,i):e}function S1(t){let n=t.get(lt,null);if(n===null)throw new x(407,!1);let e=t.get(Ox,null),i=t.get(ki,null),r=t.get(li,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function I1(t,n){let e=Hx(t);return YD(n,e,e==="svg"?gg:e==="math"?oC:null)}function Hx(t){return(t.selectors[0][0]||"div").toLowerCase()}var to=class extends xf{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=D1(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=x1(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=rN(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Ue(Fe.DynamicComponentStart);let a=ie(null);try{let l=this.componentDef,c=E1(l,r||this.ngModule,n),u=S1(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(Ux(l),()=>this.createComponentRef(u,c,e,i,o,s)):this.createComponentRef(u,c,e,i,o,s)}finally{ie(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,l=k1(r,a,s,o),c=n.rendererFactory.createRenderer(null,a),u=r?kN(c,r,a.encapsulation,e):I1(a,c),f=s?.some(nD)||o?.some(_=>typeof _!="function"&&_.bindings.some(nD)),h=Z_(null,l,null,512|rx(a),null,null,n,c,e,null,zD(u,e,!0));h[Je]=u,Nu(h);let m=null;try{let _=uv(Je,h,2,"#host",()=>l.directiveRegistry,!0,0);XD(c,u,_),Js(u,h),wf(l,h,_),z_(l,_,h),fv(l,_),i!==void 0&&T1(_,this.ngContentSelectors,i),m=Pn(_.index,h),h[ft]=m[ft],lv(l,h,null)}catch(_){throw m!==null&&i_(m),i_(h),_}finally{Ue(Fe.DynamicComponentEnd),Fu()}return new rf(this.componentType,h,!!f)}};function k1(t,n,e,i){let r=t?["ng-version","21.2.15"]:oN(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[y_].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let h of f.bindings){a+=h[y_].requiredVars;let m=u+1;h.create&&(h.targetIdx=m,(o??=[]).push(h)),h.update&&(h.targetIdx=m,(s??=[]).push(h))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,h=bu(f);l.push(h)}return K_(0,null,M1(o,s),1,a,l,null,null,null,[r],null)}function M1(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function nD(t){let n=t[y_].kind;return n==="input"||n==="twoWay"}var rf=class extends Rx{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Iu(e[re],Je),this.location=na(this._tNode,e),this.instance=Pn(this._tNode.index,e)[ft],this.hostView=this.changeDetectorRef=new eo(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Cf(i,r[re],r,n,e);this.previousInputValues.set(n,e);let s=Pn(i.index,r);cv(s,1)}get injector(){return new Qo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function T1(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var ot=(()=>{class t{static __NG_ELEMENT_ID__=A1}return t})();function A1(){let t=Mt();return $x(t,ce())}var b_=class t extends ot{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return na(this._hostTNode,this._hostLView)}get injector(){return new Qo(this._hostTNode,this._hostLView)}get parentInjector(){let n=L_(this._hostTNode,this._hostLView);if(CD(n)){let e=Ku(n,this._hostLView),i=Yu(n),r=e[re].data[i+8];return new Qo(r,e)}else return new Qo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=iD(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-at}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=ef(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,ea(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l=n&&!HR(n),c;if(l)c=e;else{let E=e||{};c=E.index,i=E.injector,r=E.projectableNodes,o=E.environmentInjector||E.ngModuleRef,s=E.directives,a=E.bindings}let u=l?n:new to(Mi(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let M=(l?f:this.parentInjector).get(ze,null);M&&(o=M)}let h=Mi(u.componentType??{}),m=ef(this._lContainer,h?.id??null),_=m?.firstChild??null,D=u.create(f,r,_,o,s,a);return this.insertImpl(D.hostView,c,ea(this._hostTNode,m)),D}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(aC(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[vt],c=new t(l,l[tn],l[vt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Kl(s,r,o,i),n.attachToViewContainerRef(),sg(Wg(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=iD(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Vl(this._lContainer,e);i&&(Dl(Wg(this._lContainer),e),yf(i[re],i))}detach(n){let e=this._adjustIndex(n,-1),i=Vl(this._lContainer,e);return i&&Dl(Wg(this._lContainer),e)!=null?new eo(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function iD(t){return t[El]}function Wg(t){return t[El]||(t[El]=[])}function $x(t,n){let e,i=n[t.index];return ni(i)?e=i:(e=Ix(i,n,null,t),n[t.index]=e,X_(n,e)),O1(e,n,t,i),new b_(e,t,n)}function R1(t,n){let e=t[Ye],i=e.createComment(""),r=Fn(n,t),o=e.parentNode(r);return Ju(e,o,i,e.nextSibling(r),!1),i}var O1=P1,N1=()=>!1;function F1(t,n,e){return N1(t,n,e)}function P1(t,n,e,i){if(t[Zr])return;let r;e.type&8?r=Nn(i):r=R1(n,e),t[Zr]=r}var w_=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},C_=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)pv(n,e).matches!==null&&this.queries[e].setDirty()}},of=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=z1(n):this.predicate=n}},D_=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},x_=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,L1(e,o)),this.matchTNodeWithReadOption(n,e,Gu(e,n,o,!1,!1))}else i===Xe?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Gu(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===O||r===ot||r===Xe&&e.type&4)this.addMatch(e.index,-2);else{let o=Gu(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function L1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function B1(t,n){return t.type&11?na(t,n):t.type&4?Df(t,n):null}function V1(t,n,e,i){return e===-1?B1(n,t):e===-2?j1(t,n,i):Pl(t,t[re],e,n)}function j1(t,n,e){if(e===O)return na(n,t);if(e===Xe)return Df(n,t);if(e===ot)return $x(n,t)}function Gx(t,n,e,i){let r=n[Ai].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let u=o[c];a.push(V1(n,u,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function E_(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=Gx(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],u=n[-l];for(let f=at;f<u.length;f++){let h=u[f];h[Yr]===h[vt]&&E_(h[re],h,c,i)}if(u[$o]!==null){let f=u[$o];for(let h=0;h<f.length;h++){let m=f[h];E_(m[re],m,c,i)}}}}}return i}function hv(t,n){return t[Ai].queries[n].queryList}function Wx(t,n,e){let i=new cr((e&4)===4);return dC(t,n,i,i.destroy),(n[Ai]??=new C_).queries.push(new w_(i))-1}function qx(t,n,e){let i=et();return i.firstCreatePass&&(Yx(i,new of(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Wx(i,ce(),n)}function Qx(t,n,e,i){let r=et();if(r.firstCreatePass){let o=Mt();Yx(r,new of(n,e,i),o.index),U1(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Wx(r,ce(),e)}function z1(t){return t.split(",").map(n=>n.trim())}function Yx(t,n,e){t.queries===null&&(t.queries=new D_),t.queries.track(new x_(n,e))}function U1(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function pv(t,n){return t.queries.getByIndex(n)}function Kx(t,n){let e=t[re],i=pv(e,n);return i.crossesNgTemplate?E_(e,t,n,[]):Gx(e,t,i,n)}function Zx(t,n,e){let i,r=il(()=>{i._dirtyCounter();let o=H1(i,t);if(n&&o===void 0)throw new x(-951,!1);return o});return i=r[dt],i._dirtyCounter=X(0),i._flatValue=void 0,r}function gv(t){return Zx(!0,!1,t)}function _v(t){return Zx(!0,!0,t)}function Xx(t,n){let e=t[dt];e._lView=ce(),e._queryIndex=n,e._queryList=hv(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function H1(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[fe]&4)return n?void 0:zt;let r=hv(e,i),o=Kx(e,i);return r.reset(o,AD),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var Pi=class{},Ef=class{};var sf=class extends Pi{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new nf(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=ig(n);this._bootstrapComponents=tx(o.bootstrap),this._r3Injector=Lg(n,e,[{provide:Pi,useValue:this},{provide:Zl,useValue:this.componentFactoryResolver},...i],wl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},af=class extends Ef{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new sf(this.moduleType,n,[])}};var jl=class extends Pi{injector;componentFactoryResolver=new nf(this);instance=null;constructor(n){super();let e=new Bo([...n.providers,{provide:Pi,useValue:this},{provide:Zl,useValue:this.componentFactoryResolver}],n.parent||Hs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ra(t,n,e=null){return new jl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var $1=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=cg(!1,e.type),r=i.length>0?ra([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=C({token:t,providedIn:"environment",factory:()=>new t(V(ze))})}return t})();function T(t){return Hl(()=>{let n=Jx(t),e=oe(w({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===B_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get($1).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||si.Emulated,styles:t.styles||zt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Vn("NgStandalone"),eE(e);let i=t.dependencies;return e.directiveDefs=rD(i,G1),e.pipeDefs=rD(i,$w),e.id=Q1(e),e})}function G1(t){return Mi(t)||bu(t)}function B(t){return Hl(()=>({type:t.type,bootstrap:t.bootstrap||zt,declarations:t.declarations||zt,imports:t.imports||zt,exports:t.exports||zt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function W1(t,n){if(t==null)return Jn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=gf.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function q1(t){if(t==null)return Jn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function S(t){return Hl(()=>{let n=Jx(t);return eE(n),n})}function vv(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Jx(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Jn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||zt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:W1(t.inputs,n),outputs:q1(t.outputs),debugInfo:null}}function eE(t){t.features?.forEach(n=>n(t))}function rD(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function Q1(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function yv(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=Y1,e.hostDirectives=i?t.map(S_):[t]):i?e.hostDirectives.unshift(...t.map(S_)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function Y1(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,tE(s,n,i),r.set(s,[a,n.length-1])}o===0&&ii(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function tE(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)oD(S_(o),n,e)}else oD(i,n,e)}function oD(t,n,e){let i=bu(t.directive);K1(i.declaredInputs,t.inputs),tE(i,n,e),e.set(i,t),n.push(i)}function S_(t){return typeof t=="function"?{directive:_t(t),inputs:Jn,outputs:Jn}:{directive:_t(t.directive),inputs:sD(t.inputs),outputs:sD(t.outputs)}}function sD(t){if(t===void 0||t.length===0)return Jn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function K1(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function Z1(t){return Object.getPrototypeOf(t.prototype).constructor}function J(t){let n=Z1(t.type),e=!0,i=[t];for(;n;){let r;if(ii(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new x(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=qg(t.inputs),s.declaredInputs=qg(t.declaredInputs),s.outputs=qg(t.outputs);let a=r.hostBindings;a&&nF(t,a);let l=r.viewQuery,c=r.contentQueries;if(l&&eF(t,l),c&&tF(t,c),X1(t,r),Hw(t.outputs,r.outputs),ii(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===J&&(e=!1)}}n=Object.getPrototypeOf(n)}J1(i)}function X1(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function J1(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Xs(r.hostAttrs,e=Xs(e,r.hostAttrs))}}function qg(t){return t===Jn?{}:t===zt?[]:t}function eF(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function tF(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function nF(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function nE(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Xs(t.mergedAttrs,t.attrs);let u=t.tView=K_(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Qs(t,!1);let l=rF(e,n,t,i);Pu()&&iv(e,n,l,t),Js(l,n);let c=Ix(l,n,l,t);n[i+Je]=c,X_(n,c),F1(c,t,n)}function iF(t,n,e,i,r,o,s,a,l,c,u){let f=e+Je,h;return n.firstCreatePass?(h=ia(n,f,4,s||null,a||null),Tu()&&Nx(n,t,h,Ln(n.consts,c),ov),yD(n,h)):h=n.data[f],nE(h,t,n,e,i,r,o,l),Ws(h)&&wf(n,t,h),c!=null&&Ql(t,h,u),h}function zl(t,n,e,i,r,o,s,a,l,c,u){let f=e+Je,h;if(n.firstCreatePass){if(h=ia(n,f,4,s||null,a||null),c!=null){let m=Ln(n.consts,c);h.localNames=[];for(let _=0;_<m.length;_+=2)h.localNames.push(m[_],-1)}}else h=n.data[f];return nE(h,t,n,e,i,r,o,l),c!=null&&Ql(t,h,u),h}function se(t,n,e,i,r,o,s,a){let l=ce(),c=et(),u=Ln(c.consts,o);return iF(l,c,t,n,e,i,r,u,void 0,s,a),se}var rF=oF;function oF(t,n,e,i){return Ml(!0),n[Ye].createComment("")}var Sf=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function mr(t){return typeof t=="function"&&t[dt]!==void 0}var bv=new b("");function ro(t){return!!t&&typeof t.then=="function"}function wv(t){return!!t&&typeof t.subscribe=="function"}var Cv=new b("");function If(t){return Ti([{provide:Cv,multi:!0,useValue:t}])}var Dv=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(Cv,{optional:!0})??[];injector=d(Y);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=yt(this.injector,r);if(ro(o))e.push(o);else if(wv(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Xl=new b("");function iE(){vp(()=>{let t="";throw new x(600,t)})}function rE(t){return t.isBoundToModule}var sF=10;var pn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(En);afterRenderManager=d(vf);zonelessEnabled=d(Tl);rootEffectScheduler=d(Bu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new I;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(ar);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ee(e=>!e))}constructor(){d(li,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(ze);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=Y.NULL){return this._injector.get(z).run(()=>{Ue(Fe.BootstrapComponentStart);let s=e instanceof xf;if(!this._injector.get(Dv).done){let _="";throw new x(405,_)}let l;s?l=e:l=this._injector.get(Zl).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=rE(l)?void 0:this._injector.get(Pi),u=i||l.selector,f=l.create(r,[],u,c),h=f.location.nativeElement,m=f.injector.get(bv,null);return m?.registerApplication(h),f.onDestroy(()=>{this.detachView(f.hostView),Fl(this.components,f),m?.unregisterApplication(h)}),this._loadComponent(f),Ue(Fe.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Ue(Fe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(_f.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Ue(Fe.ChangeDetectionEnd),new x(101,!1);let e=ie(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ie(e),this.afterTick.next(),Ue(Fe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(lt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<sF;){Ue(Fe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Ue(Fe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Sl(r))continue;let o=i&&!this.zonelessEnabled?0:1;Dx(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Sl(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Fl(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Xl,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Fl(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new x(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Fl(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function kf(t,n){let e=ce(),i=Xr();if(ai(e,i,n)){let r=et(),o=kl();if(Cf(o,r,e,t,n))Ri(o)&&gx(e,o.index);else{let a=Fn(o,e);_x(e[Ye],a,null,o.value,t,n,null)}}return kf}function te(t,n,e,i){let r=ce(),o=Xr();if(ai(r,o,n)){let s=et(),a=kl();PN(a,r,t,n,e,i)}return te}var I_=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Qg(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function aF(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){ie(i);let c=n.length-1;for(ie(null);s<=a&&s<=c;){let u=t.at(s),f=n[s],h=Qg(s,u,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let m=t.at(a),_=n[c],D=Qg(a,m,c,_,e);if(D!==0){D<0&&t.updateValue(a,_),a--,c--;continue}let E=e(s,u),M=e(a,m),Z=e(s,f);if(Object.is(Z,M)){let Te=e(c,_);Object.is(Te,E)?(t.swap(s,a),t.updateValue(a,_),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new lf,o??=lD(t,s,a,e),k_(t,r,s,Z))t.updateValue(s,f),s++,a++;else if(o.has(Z))r.set(E,t.detach(s)),a--;else{let Te=t.create(s,n[s]);t.attach(s,Te),s++,a++}}for(;s<=c;)aD(t,r,e,s,n[s]),s++}else if(n!=null){ie(i);let c=n[Symbol.iterator]();ie(null);let u=c.next();for(;!u.done&&s<=a;){let f=t.at(s),h=u.value,m=Qg(s,f,s,h,e);if(m!==0)m<0&&t.updateValue(s,h),s++,u=c.next();else{r??=new lf,o??=lD(t,s,a,e);let _=e(s,h);if(k_(t,r,s,_))t.updateValue(s,h),s++,a++,u=c.next();else if(!o.has(_))t.attach(s,t.create(s,h)),s++,a++,u=c.next();else{let D=e(s,f);r.set(D,t.detach(s)),a--}}}for(;!u.done;)aD(t,r,e,t.length,u.value),u=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function k_(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function aD(t,n,e,i,r){if(k_(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function lD(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var lf=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function me(t,n,e,i,r,o,s,a){Vn("NgControlFlow");let l=ce(),c=et(),u=Ln(c.consts,o);return zl(l,c,t,n,e,i,r,u,256,s,a),xv}function xv(t,n,e,i,r,o,s,a){Vn("NgControlFlow");let l=ce(),c=et(),u=Ln(c.consts,o);return zl(l,c,t,n,e,i,r,u,512,s,a),xv}function he(t,n){Vn("NgControlFlow");let e=ce(),i=Xr(),r=e[i]!==rn?e[i]:-1,o=r!==-1?cf(e,Je+r):void 0,s=0;if(ai(e,i,t)){let a=ie(null);try{if(o!==void 0&&Mx(o,s),t!==-1){let l=Je+t,c=cf(e,l),u=R_(e[re],l),f=Ax(c,u,e),h=Yl(e,u,n,{dehydratedView:f});Kl(c,h,s,ea(u,f))}}finally{ie(a)}}else if(o!==void 0){let a=kx(o,s);a!==void 0&&(a[ft]=n)}}var M_=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-at}};function Ev(t,n){return n}var T_=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Sv(t,n,e,i,r,o,s,a,l,c,u,f,h){Vn("NgControlFlow");let m=ce(),_=et(),D=l!==void 0,E=ce(),M=a?s.bind(E[nn][ft]):s,Z=new T_(D,M);E[Je+t]=Z,zl(m,_,t+1,n,e,i,r,Ln(_.consts,o),256),D&&zl(m,_,t+2,l,c,u,f,Ln(_.consts,h),512)}var A_=class extends I_{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-at}at(n){return this.getLView(n)[ft].$implicit}attach(n,e){let i=e[jo];this.needsIndexUpdate||=n!==this.length,Kl(this.lContainer,e,n,ea(this.templateTNode,i)),lF(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,cF(this.lContainer,n),dF(this.lContainer,n)}create(n,e){let i=ef(this.lContainer,this.templateTNode.tView.ssrId);return Yl(this.hostLView,this.templateTNode,new M_(this.lContainer,e,n),{dehydratedView:i})}destroy(n){yf(n[re],n)}updateValue(n,e){this.getLView(n)[ft].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[ft].$index=n}getLView(n){return uF(this.lContainer,n)}};function Iv(t){let n=ie(null),e=Oi();try{let i=ce(),r=i[re],o=i[e],s=e+1,a=cf(i,s);if(o.liveCollection===void 0){let c=R_(r,s);o.liveCollection=new A_(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(aF(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Xr(),u=l.length===0;if(ai(i,c,u)){let f=e+2,h=cf(i,f);if(u){let m=R_(r,f),_=Ax(h,m,i),D=Yl(i,m,void 0,{dehydratedView:_});Kl(h,D,0,ea(m,_))}else r.firstUpdatePass&&o1(h),Mx(h,0)}}}finally{ie(n)}}function cf(t,n){return t[n]}function lF(t,n){if(t.length<=at)return;let e=at+n,i=t[e],r=i?i[Kr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[rr];hN(o,r),Ko.delete(i[or]),r.detachedLeaveAnimationFns=void 0}}function cF(t,n){if(t.length<=at)return;let e=at+n,i=t[e],r=i?i[Kr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function dF(t,n){return Vl(t,n)}function uF(t,n){return kx(t,n)}function R_(t,n){return Iu(t,n)}function k(t,n,e){let i=ce(),r=Xr();if(ai(i,r,n)){let o=et(),s=kl();RN(s,i,t,n,i[Ye],e)}return k}function O_(t,n,e,i,r){Cf(n,t,e,r?"class":"style",i)}function p(t,n,e,i){let r=ce(),o=r[re],s=t+Je,a=o.firstCreatePass?uv(s,r,2,n,ov,Tu(),e,i):o.data[s];if(Ri(a)){let l=r[ti].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(Ux(c),()=>(cD(t,n,r,a,i),p))}}return cD(t,n,r,a,i),p}function cD(t,n,e,i,r){if(sv(i,e,t,n,oE),Ws(i)){let o=e[re];wf(o,e,i),z_(o,i,e)}r!=null&&Ql(e,i)}function g(){let t=et(),n=Mt(),e=av(n);return t.firstCreatePass&&fv(t,e),Ig(e)&&kg(),Eg(),e.classesWithoutHost!=null&&YR(e)&&O_(t,e,ce(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&KR(e)&&O_(t,e,ce(),e.stylesWithoutHost,!1),g}function F(t,n,e,i){return p(t,n,e,i),g(),F}function Tt(t,n,e,i){let r=ce(),o=r[re],s=t+Je,a=o.firstCreatePass?y1(s,o,2,n,e,i):o.data[s];return sv(a,r,t,n,oE),i!=null&&Ql(r,a),Tt}function At(){let t=Mt(),n=av(t);return Ig(n)&&kg(),Eg(),At}function gn(t,n,e,i){return Tt(t,n,e,i),At(),gn}var oE=(t,n,e,i,r)=>(Ml(!0),YD(n[Ye],i,Pg()));function $t(t,n,e){let i=ce(),r=i[re],o=t+Je,s=r.firstCreatePass?uv(o,i,8,"ng-container",ov,Tu(),n,e):r.data[o];if(sv(s,i,t,"ng-container",fF),Ws(s)){let a=i[re];wf(a,i,s),z_(a,s,i)}return e!=null&&Ql(i,s),$t}function Gt(){let t=et(),n=Mt(),e=av(n);return t.firstCreatePass&&fv(t,e),Gt}function Wt(t,n,e){return $t(t,n,e),Gt(),Wt}var fF=(t,n,e,i,r)=>(Ml(!0),HO(n[Ye],""));function Ze(){return ce()}function qt(t,n,e){let i=ce(),r=Xr();if(ai(i,r,n)){let o=et(),s=kl();px(s,i,t,n,i[Ye],e)}return qt}var Rl=void 0;function mF(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var hF=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Rl,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Rl,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Rl,Rl,Rl],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",mF],Yg={};function Mf(t){let n=pF(t),e=dD(n);if(e)return e;let i=n.split("-")[0];if(e=dD(i),e)return e;if(i==="en")return hF;throw new x(701,!1)}function dD(t){return t in Yg||(Yg[t]=Rn.ng&&Rn.ng.common&&Rn.ng.common.locales&&Rn.ng.common.locales[t]),Yg[t]}var es=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(es||{});function pF(t){return t.toLowerCase().replace(/_/g,"-")}var Jl="en-US";var gF=Jl;function sE(t){typeof t=="string"&&(gF=t.toLowerCase().replace(/_/g,"-"))}function A(t,n,e){let i=ce(),r=et(),o=Mt();return _F(r,i,i[Ye],o,t,n,e),A}function Tf(t,n,e){let i=ce(),r=et(),o=Mt();return(o.type&3||e)&&jx(o,r,i,e,i[Ye],t,n,qu(o,i,n)),Tf}function _F(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=qu(i,n,o),jx(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let h=u[f],m=u[f+1];l??=qu(i,n,o),tD(i,n,h,m,r,l)}if(c&&c.length)for(let f of c)l??=qu(i,n,o),tD(i,n,f,r,r,l)}}function W(t=1){return xC(t)}function vF(t,n){let e=null,i=JO(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?ix(t,o,!0):nN(i,o))return r}return e}function xe(t){let n=ce()[nn][tn];if(!n.projection){let e=t?t.length:1,i=n.projection=Kw(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?vF(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function U(t,n=0,e,i,r,o){let s=ce(),a=et(),l=i?t+1:null;l!==null&&zl(s,a,l,i,r,o,null,e);let c=ia(a,Je+t,16,null,e||null);c.projection===null&&(c.projection=n),Ag();let f=!s[jo]||Sg();s[nn][tn].projection[c.projection]===null&&l!==null?yF(s,a,l):f&&!hf(c)&&EN(a,s,c)}function yF(t,n,e){let i=Je+e,r=n.data[i],o=t[i],s=ef(o,r.tView.ssrId),a=Yl(t,r,void 0,{dehydratedView:s});Kl(o,a,0,ea(r,s))}function wt(t,n,e,i){return Qx(t,n,e,i),wt}function He(t,n,e){return qx(t,n,e),He}function H(t){let n=ce(),e=et(),i=Ou();Il(i+1);let r=pv(e,i);if(t.dirty&&sC(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Kx(n,i);t.reset(o,AD),t.notifyOnChanges()}return!0}return!1}function $(){return hv(ce(),Ou())}function Af(t,n,e,i,r){return Xx(n,Qx(t,e,i,r)),Af}function Rf(t,n,e,i){return Xx(t,qx(n,e,i)),Rf}function Of(t=1){Il(Ou()+t)}function gt(t){let n=hC();return vg(n,Je+t)}function Uu(t,n){return t<<17|n<<2}function Zo(t){return t>>17&32767}function bF(t){return(t&2)==2}function wF(t,n){return t&131071|n<<17}function N_(t){return t|2}function ta(t){return(t&131068)>>2}function Kg(t,n){return t&-131069|n<<2}function CF(t){return(t&1)===1}function F_(t){return t|1}function DF(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Zo(s),l=ta(s);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||Us(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let h=Zo(t[a+1]);t[i+1]=Uu(h,a),h!==0&&(t[h+1]=Kg(t[h+1],i)),t[a+1]=wF(t[a+1],i)}else t[i+1]=Uu(a,0),a!==0&&(t[a+1]=Kg(t[a+1],i)),a=i;else t[i+1]=Uu(l,0),a===0?a=i:t[l+1]=Kg(t[l+1],i),l=i;c&&(t[i+1]=N_(t[i+1])),uD(t,u,i,!0),uD(t,u,i,!1),xF(n,u,t,i,o),s=Uu(a,l),o?n.classBindings=s:n.styleBindings=s}function xF(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Us(o,n)>=0&&(e[i+1]=F_(e[i+1]))}function uD(t,n,e,i){let r=t[e+1],o=n===null,s=i?Zo(r):ta(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];EF(l,n)&&(a=!0,t[s+1]=i?F_(c):N_(c)),s=i?Zo(c):ta(c)}a&&(t[e+1]=i?N_(r):F_(r))}function EF(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Us(t,n)>=0:!1}var oi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function SF(t){return t.substring(oi.key,oi.keyEnd)}function IF(t){return kF(t),aE(t,lE(t,0,oi.textEnd))}function aE(t,n){let e=oi.textEnd;return e===n?-1:(n=oi.keyEnd=MF(t,oi.key=n,e),lE(t,n,e))}function kF(t){oi.key=0,oi.keyEnd=0,oi.value=0,oi.valueEnd=0,oi.textEnd=t.length}function lE(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function MF(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function hr(t,n,e){return cE(t,n,e,!1),hr}function N(t,n){return cE(t,n,null,!0),N}function on(t){AF(LF,TF,t,!0)}function TF(t,n){for(let e=IF(n);e>=0;e=aE(n,e))xu(t,SF(n),!0)}function cE(t,n,e,i){let r=ce(),o=et(),s=Au(2);if(o.firstUpdatePass&&uE(o,t,s,i),n!==rn&&ai(r,s,n)){let a=o.data[Oi()];fE(o,a,r,r[Ye],t,r[s+1]=VF(n,e),i,s)}}function AF(t,n,e,i){let r=et(),o=Au(2);r.firstUpdatePass&&uE(r,null,o,i);let s=ce();if(e!==rn&&ai(s,o,e)){let a=r.data[Oi()];if(mE(a,i)&&!dE(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=vu(l,e||"")),O_(r,a,s,e,i)}else BF(r,a,s,s[Ye],s[o+1],s[o+1]=PF(t,n,e),i,o)}}function dE(t,n){return n>=t.expandoStartIndex}function uE(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Oi()],s=dE(t,e);mE(o,i)&&n===null&&!s&&(n=!1),n=RF(r,o,n,i),DF(r,o,n,e,s,i)}}function RF(t,n,e,i){let r=bC(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Zg(null,t,n,e,i),e=Ul(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Zg(r,t,n,e,i),o===null){let l=OF(t,n,i);l!==void 0&&Array.isArray(l)&&(l=Zg(null,t,n,l[1],i),l=Ul(l,n.attrs,i),NF(t,n,i,l))}else o=FF(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function OF(t,n,e){let i=e?n.classBindings:n.styleBindings;if(ta(i)!==0)return t[Zo(i)]}function NF(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Zo(r)]=i}function FF(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Ul(i,s,e)}return Ul(i,n.attrs,e)}function Zg(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Ul(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Ul(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),xu(t,s,e?!0:n[++o]))}return t===void 0?null:t}function PF(t,n,e){if(e==null||e==="")return zt;let i=[],r=Bn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function LF(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&xu(t,i,e)}function BF(t,n,e,i,r,o,s,a){r===rn&&(r=zt);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,m=c<o.length?o[c+1]:void 0,_=null,D;u===f?(l+=2,c+=2,h!==m&&(_=f,D=m)):f===null||u!==null&&u<f?(l+=2,_=u):(c+=2,_=f,D=m),_!==null&&fE(t,n,e,i,_,D,s,a),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function fE(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],u=CF(c)?fD(l,n,e,r,ta(c),s):void 0;if(!df(u)){df(o)||bF(c)&&(o=fD(l,null,e,r,a,s));let f=_g(Oi(),e);IN(i,s,f,r,o)}}function fD(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,h=e[r+1];h===rn&&(h=f?zt:void 0);let m=f?Eu(h,i):u===i?h:void 0;if(c&&!df(m)&&(m=Eu(l,i)),df(m)&&(a=m,s))return a;let _=t[r+1];r=s?Zo(_):ta(_)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=Eu(l,i))}return a}function df(t){return t!==void 0}function VF(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=wl(Bn(t)))),t}function mE(t,n){return(t.flags&(n?8:16))!==0}function y(t,n=""){let e=ce(),i=et(),r=t+Je,o=i.firstCreatePass?ia(i,r,1,n,null):i.data[r],s=jF(i,e,o,n);e[r]=s,Pu()&&iv(i,e,s,o),Qs(o,!1)}var jF=(t,n,e,i)=>(Ml(!0),zO(n[Ye],i));function zF(t,n,e,i=""){return ai(t,Xr(),e)?n+zs(e)+i:rn}function UF(t,n,e,i,r,o=""){let s=pC(),a=Vx(t,s,e,r);return Au(2),a?n+zs(e)+i+zs(r)+o:rn}function Pt(t){return Oe("",t),Pt}function Oe(t,n,e){let i=ce(),r=zF(i,t,n,e);return r!==rn&&hE(i,Oi(),r),Oe}function Nf(t,n,e,i,r){let o=ce(),s=UF(o,t,n,e,i,r);return s!==rn&&hE(o,Oi(),s),Nf}function hE(t,n,e){let i=_g(n,t);UO(t[Ye],i,e)}function mD(t,n,e){let i=et();i.firstCreatePass&&pE(n,i.data,i.blueprint,ii(t),e)}function pE(t,n,e,i,r){if(t=_t(t),Array.isArray(t))for(let o=0;o<t.length;o++)pE(t[o],n,e,i,r);else{let o=et(),s=ce(),a=Mt(),l=Lo(t)?t:_t(t.provide),c=ug(t),u=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(Lo(t)||!t.multi){let m=new Yo(c,r,R,null),_=Jg(l,n,r?u:u+h,f);_===-1?(t_(Xu(a,s),o,l),Xg(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(m),s.push(m)):(e[_]=m,s[_]=m)}else{let m=Jg(l,n,u+h,f),_=Jg(l,n,u,u+h),D=m>=0&&e[m],E=_>=0&&e[_];if(r&&!E||!r&&!D){t_(Xu(a,s),o,l);let M=GF(r?$F:HF,e.length,r,i,c,t);!r&&E&&(e[_].providerFactory=M),Xg(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(M),s.push(M)}else{let M=gE(e[r?_:m],c,!r&&i);Xg(o,t,m>-1?m:_,M)}!r&&i&&E&&e[_].componentProviders++}}}function Xg(t,n,e,i){let r=Lo(n),o=nC(n);if(r||o){let l=(o?_t(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function gE(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Jg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function HF(t,n,e,i,r){return P_(this.multi,[])}function $F(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=Pl(i,i[re],this.providerFactory.index,r);s=l.slice(0,a),P_(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],P_(o,s);return s}function P_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function GF(t,n,e,i,r,o){let s=new Yo(t,e,R,null);return s.multi=[],s.index=n,s.componentProviders=0,gE(s,r,i&&!e),s}function ge(t,n){return e=>{e.providersResolver=(i,r)=>mD(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>mD(i,r?r(n):n,!0))}}function pr(t,n){let e=Og()+t,i=ce();return i[e]===rn?Bx(i,e,n()):b1(i,e)}function WF(t,n){let e=t[n];return e===rn?void 0:e}function qF(t,n,e,i,r,o,s){let a=n+e;return Vx(t,a,r,o)?Bx(t,a+2,s?i.call(s,r,o):i(r,o)):WF(t,a+2)}function Ff(t,n){let e=et(),i,r=t+Je;e.firstCreatePass?(i=QF(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=Gr(i.type,!0)),s,a=Jt(R);try{let l=Zu(!1),c=o();return Zu(l),yg(e,ce(),r,c),c}finally{Jt(a)}}function QF(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Pf(t,n,e,i){let r=t+Je,o=ce(),s=vg(o,r);return YF(o,r)?qF(o,Og(),n,s.transform,e,i,s):s.transform(e,i)}function YF(t,n){return t[re].data[n].pure}function Lf(t,n){return Df(t,n)}var uf=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},kv=(()=>{class t{compileModuleSync(e){return new af(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=ig(e),o=tx(r.declarations).reduce((s,a)=>{let l=Mi(a);return l&&s.push(new to(l)),s},[]);return new uf(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _E=(()=>{class t{applicationErrorHandler=d(En);appRef=d(pn);taskService=d(ar);ngZone=d(z);zonelessEnabled=d(Tl);tracing=d(li,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Se;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(yl):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Ug,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?kC:Bg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(yl+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function vE(){return[{provide:ki,useExisting:_E},{provide:z,useClass:bl},{provide:Tl,useValue:!0}]}function KF(){return typeof $localize<"u"&&$localize.locale||Jl}var ec=new b("",{factory:()=>d(ec,{optional:!0,skipSelf:!0})||KF()});function Le(t){return Bw(t)}function sn(t,n){return il(t,n?.equal)}var ZF=t=>t;function Mv(t,n){if(typeof t=="function"){let e=Lp(t,ZF,n?.equal);return yE(e,n?.debugName)}else{let e=Lp(t.source,t.computation,t.equal);return yE(e,t.debugName)}}function yE(t,n){let e=t[dt],i=t;return i.set=r=>Pw(e,r),i.update=r=>Lw(e,r),i.asReadonly=Lu.bind(t),i}var SE=Symbol("InputSignalNode#UNSET"),dP=oe(w({},rl),{transformFn:void 0,applyValueToInputSignal(t,n){Io(t,n)}});function IE(t,n){let e=Object.create(dP);e.value=t,e.transformFn=n?.transform;function i(){if(Lr(e),e.value===SE){let r=null;throw new x(-950,r)}return e.value}return i[dt]=e,i}var _n=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>$l(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function bE(t,n){return IE(t,n)}function uP(t){return IE(SE,t)}var kE=(bE.required=uP,bE);function wE(t,n){return gv(n)}function fP(t,n){return _v(n)}var nc=(wE.required=fP,wE);function CE(t,n){return gv(n)}function mP(t,n){return _v(n)}var ME=(CE.required=mP,CE);var Av=new b(""),hP=new b("");function tc(t){return!t.moduleRef}function pP(t){let n=tc(t)?t.r3Injector:t.moduleRef.injector,e=n.get(z);return e.run(()=>{tc(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(En),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),tc(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Av);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Av);s.add(o),t.moduleRef.onDestroy(()=>{Fl(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return _P(i,e,()=>{let o=n.get(ar),s=o.add(),a=n.get(Dv);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(ec,Jl);if(sE(l||Jl),!n.get(hP,!0))return tc(t)?n.get(pn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(tc(t)){let u=n.get(pn);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return gP?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var gP;function _P(t,n,e){try{let i=e();return ro(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Bf=null;function vP(t=[],n){return Y.create({name:n,providers:[{provide:xl,useValue:"platform"},{provide:Av,useValue:new Set([()=>Bf=null])},...t]})}function yP(t=[]){if(Bf)return Bf;let n=vP(t);return Bf=n,iE(),bP(n),n}function bP(t){let n=t.get(ff,null);yt(t,()=>{n?.forEach(e=>e())})}var wP=1e4;var _X=wP-1e3;var Ce=(()=>{class t{static __NG_ELEMENT_ID__=CP}return t})();function CP(t){return DP(Mt(),ce(),(t&16)===16)}function DP(t,n,e){if(Ri(t)&&!e){let i=Pn(t.index,n);return new eo(i,i)}else if(t.type&175){let i=n[nn];return new eo(i,n)}return null}var Rv=class{supports(n){return mv(n)}create(n){return new Ov(n)}},xP=(t,n)=>n,Ov=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||xP}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<DE(i,r,o)?e:i,a=DE(s,r,o),l=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let c=a-r,u=l-r;if(c!=u){for(let h=0;h<c;h++){let m=h<o.length?o[h]:o[h]=0,_=m+h;u<=_&&_<c&&(o[h]=m+1)}let f=s.previousIndex;o[f]=u-c}}a!==l&&n(s,a,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!mv(n))throw new x(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,Px(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new Nv(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Vf),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Vf),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Nv=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},Fv=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Vf=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new Fv,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function DE(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function xE(){return new Li([new Rv])}var Li=(()=>{class t{factories;static \u0275prov=C({token:t,providedIn:"root",factory:xE});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||xE())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new x(901,!1)}}return t})();function TE(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Ue(Fe.BootstrapApplicationStart);try{let o=r?.injector??yP(i),s=[vE(),TC,...e||[]],a=new jl({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return pP({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Ue(Fe.BootstrapApplicationEnd)}}function j(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function an(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Tv=Symbol("NOT_SET"),AE=new Set,EP=oe(w({},rl),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Tv,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Tv&&!Ts(this))return this.signal;try{for(let r of this.cleanup??AE)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Ji(this),i;try{i=this.userFn.apply(null,n)}finally{Br(this,e)}return(this.value===Tv||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Pv=class extends Ll{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Ht),s),this.scheduler=r;for(let a of ev){let l=e[a];if(l===void 0)continue;let c=Object.create(EP);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(Lr(c),c.value),c.signal[dt]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[a]=c,this.hooks[a]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??AE)e()}finally{Vr(n)}}};function RE(t,n){let e=n?.injector??d(Y),i=e.get(ki),r=e.get(vf),o=e.get(li,null,{optional:!0});r.impl??=e.get(tv);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Ys,null,{optional:!0}),l=new Pv(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function jf(t,n){let e=Mi(t),i=n.elementInjector||Hs();return new to(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function OE(t){let n=Mi(t);if(!n)return null;let e=new to(n);return{get selector(){return e.selector},get type(){return e.componentType},get inputs(){return e.inputs},get outputs(){return e.outputs},get ngContentSelectors(){return e.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var NE=null;function jn(){return NE}function Bv(t){NE??=t}var ic=class{},_r=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(FE),providedIn:"platform"})}return t})(),Vv=new b(""),FE=(()=>{class t extends _r{_location;_history;_doc=d(G);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return jn().getBaseHref(this._doc)}onPopState(e){let i=jn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=jn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function zf(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function PE(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function ci(t){return t&&t[0]!=="?"?`?${t}`:t}var di=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(Hf),providedIn:"root"})}return t})(),Uf=new b(""),Hf=(()=>{class t extends di{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(G).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return zf(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+ci(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+ci(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+ci(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(V(_r),V(Uf,8))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Bi=(()=>{class t{_subject=new I;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=kP(PE(LE(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+ci(i))}normalize(e){return t.stripTrailingSlash(IP(this._basePath,LE(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ci(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ci(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ci;static joinWithSlash=zf;static stripTrailingSlash=PE;static \u0275fac=function(i){return new(i||t)(V(di))};static \u0275prov=C({token:t,factory:()=>SP(),providedIn:"root"})}return t})();function SP(){return new Bi(V(di))}function IP(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function LE(t){return t.replace(/\/index.html$/,"")}function kP(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Uv=(()=>{class t extends di{_platformLocation;_baseHref="";_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,i!=null&&(this._baseHref=i)}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}path(e=!1){let i=this._platformLocation.hash??"#";return i.length>0?i.substring(1):i}prepareExternalUrl(e){let i=zf(this._baseHref,e);return i.length>0?"#"+i:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+ci(o))||this._platformLocation.pathname;this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+ci(o))||this._platformLocation.pathname;this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(V(_r),V(Uf,8))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();var Hv=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(Hv||{});var Vi={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function oa(t,n){let e=Mf(t),i=e[es.NumberSymbols][n];if(typeof i>"u"){if(n===Vi.CurrencyDecimal)return e[es.NumberSymbols][Vi.Decimal];if(n===Vi.CurrencyGroup)return e[es.NumberSymbols][Vi.Group]}return i}function zE(t,n){return Mf(t)[es.NumberFormats][n]}var MP=/^(\d+)?\.((\d+)(-(\d+))?)?$/,BE=22,$f=".",rc="0",TP=";",AP=",",jv="#";function RP(t,n,e,i,r,o,s=!1){let a="",l=!1;if(!isFinite(t))a=oa(e,Vi.Infinity);else{let c=FP(t);s&&(c=NP(c));let u=n.minInt,f=n.minFrac,h=n.maxFrac;if(o){let Z=o.match(MP);if(Z===null)throw new x(2306,!1);let Te=Z[1],Ee=Z[3],Xt=Z[5];Te!=null&&(u=zv(Te)),Ee!=null&&(f=zv(Ee)),Xt!=null?h=zv(Xt):Ee!=null&&f>h&&(h=f);let pt=100;if(u>pt||f>pt||h>pt)throw new x(2306,!1)}PP(c,f,h);let m=c.digits,_=c.integerLen,D=c.exponent,E=[];for(l=m.every(Z=>!Z);_<u;_++)m.unshift(0);for(;_<0;_++)m.unshift(0);_>0?E=m.splice(_,m.length):(E=m,m=[0]);let M=[];for(m.length>=n.lgSize&&M.unshift(m.splice(-n.lgSize,m.length).join(""));m.length>n.gSize;)M.unshift(m.splice(-n.gSize,m.length).join(""));m.length&&M.unshift(m.join("")),a=M.join(oa(e,i)),E.length&&(a+=oa(e,r)+E.join("")),D&&(a+=oa(e,Vi.Exponential)+"+"+D)}return t<0&&!l?a=n.negPre+a+n.negSuf:a=n.posPre+a+n.posSuf,a}function UE(t,n,e){let i=zE(n,Hv.Decimal),r=OP(i,oa(n,Vi.MinusSign));return RP(t,r,n,Vi.Group,Vi.Decimal,e)}function OP(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(TP),r=i[0],o=i[1],s=r.indexOf($f)!==-1?r.split($f):[r.substring(0,r.lastIndexOf(rc)+1),r.substring(r.lastIndexOf(rc)+1)],a=s[0],l=s[1]||"";e.posPre=a.substring(0,a.indexOf(jv));for(let u=0;u<l.length;u++){let f=l.charAt(u);f===rc?e.minFrac=e.maxFrac=u+1:f===jv?e.maxFrac=u+1:e.posSuf+=f}let c=a.split(AP);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,f=o.indexOf(jv);e.negPre=o.substring(0,f).replace(/'/g,""),e.negSuf=o.slice(f+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function NP(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function FP(t){let n=Math.abs(t)+"",e=0,i,r,o,s,a;for((r=n.indexOf($f))>-1&&(n=n.replace($f,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===rc;o++);if(o===(a=n.length))i=[0],r=1;else{for(a--;n.charAt(a)===rc;)a--;for(r-=o,i=[],s=0;o<=a;o++,s++)i[s]=Number(n.charAt(o))}return r>BE&&(i=i.splice(0,BE-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function PP(t,n,e){if(n>e)throw new x(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),s=o+t.integerLen,a=i[s];if(s>0){i.splice(Math.max(t.integerLen,s));for(let f=s;f<i.length;f++)i[f]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,s=o+1),i[0]=0;for(let f=1;f<s;f++)i[f]=0}if(a>=5)if(s-1<0){for(let f=0;f>s;f--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[s-1]++;for(;r<Math.max(0,o);r++)i.push(0);let l=o!==0,c=n+t.integerLen,u=i.reduceRight(function(f,h,m,_){return h=h+f,_[m]=h<10?h:h-10,l&&(_[m]===0&&m>=c?_.pop():l=!1),h>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function zv(t){let n=parseInt(t);if(isNaN(n))throw new x(2305,!1);return n}var Gf=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},sa=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Gf(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),VE(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);VE(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(R(ot),R(Xe),R(Li))};static \u0275dir=S({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function VE(t,n){t.context.$implicit=n.item}var ui=(()=>{class t{_viewContainer;_context=new Wf;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){jE(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){jE(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(R(ot),R(Xe))};static \u0275dir=S({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Wf=class{$implicit=null;ngIf=null};function jE(t,n){if(t&&!t.createEmbeddedView)throw new x(2020,!1)}var $v=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(Y);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(R(ot))};static \u0275dir=S({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Pe]})}return t})();function LP(t,n){return new x(2100,!1)}var Gv=(()=>{class t{_locale;constructor(e){this._locale=e}transform(e,i,r){if(!BP(e))return null;r||=this._locale;try{let o=VP(e);return UE(o,r,i)}catch(o){throw LP(t,o.message)}}static \u0275fac=function(i){return new(i||t)(R(ec,16))};static \u0275pipe=vv({name:"number",type:t,pure:!0})}return t})();function BP(t){return!(t==null||t===""||t!==t)}function VP(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new x(2309,!1);return t}var Rt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();function oc(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var ts=class{};var qv="browser";function HE(t){return t===qv}var Qv=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>new Wv(d(G),window)})}return t})(),Wv=class{document;window;offset=()=>[0,0];constructor(n,e){this.document=n,this.window=e}setOffset(n){Array.isArray(n)?this.offset=()=>n:this.offset=n}getScrollPosition(){return[this.window.scrollX,this.window.scrollY]}scrollToPosition(n,e){this.window.scrollTo(oe(w({},e),{left:n[0],top:n[1]}))}scrollToAnchor(n,e){let i=jP(this.document,n);i&&(this.scrollToElement(i,e),i.focus({preventScroll:!0}))}setHistoryScrollRestoration(n){try{this.window.history.scrollRestoration=n}catch{console.warn(Xn(2400,!1))}}scrollToElement(n,e){let i=n.getBoundingClientRect(),r=i.left+this.window.pageXOffset,o=i.top+this.window.pageYOffset,s=this.offset();this.window.scrollTo(oe(w({},e),{left:r-s[0],top:o-s[1]}))}};function jP(t,n){let e=t.getElementById(n)||t.getElementsByName(n)[0];if(e)return e;if(typeof t.createTreeWalker=="function"&&t.body&&typeof t.body.attachShadow=="function"){let i=t.createTreeWalker(t.body,NodeFilter.SHOW_ELEMENT),r=i.currentNode;for(;r;){let o=r.shadowRoot;if(o){let s=o.getElementById(n)||o.querySelector(`[name="${n}"]`);if(s)return s}r=i.nextNode()}}return null}var sc=class{_doc;constructor(n){this._doc=n}manager},qf=(()=>{class t extends sc{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(V(G))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Kf=new b(""),Xv=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof qf));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof qf);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new x(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(V(Kf),V(z))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Yv="ng-app-id";function $E(t){for(let n of t)n.remove()}function GE(t,n){let e=n.createElement("style");return e.textContent=t,e}function zP(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Yv}="${n}"],link[${Yv}="${n}"]`);if(r)for(let o of r)o.removeAttribute(Yv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function Zv(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Jv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,zP(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,GE);i?.forEach(r=>this.addUsage(r,this.external,Zv))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&($E(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])$E(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,GE(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Zv(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(V(G),V(no),V(Jo,8),V(Xo))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Kv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},ey=/%COMP%/g;var qE="%COMP%",UP=`_nghost-${qE}`,HP=`_ngcontent-${qE}`,$P=!0,GP=new b("",{factory:()=>$P});function WP(t){return HP.replace(ey,t)}function qP(t){return UP.replace(ey,t)}function QE(t,n){return n.map(e=>e.replace(ey,t))}var cc=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.defaultRenderer=new ac(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Yf?r.applyToHost(e):r instanceof lc&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case si.Emulated:o=new Yf(l,c,i,this.appId,u,s,a,f);break;case si.ShadowDom:return new Qf(l,e,i,s,a,this.nonce,f,c);case si.ExperimentalIsolatedShadowDom:return new Qf(l,e,i,s,a,this.nonce,f);default:o=new lc(l,c,i,u,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(V(Xv),V(Jv),V(no),V(GP),V(G),V(z),V(Jo),V(li,8))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),ac=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Kv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(WE(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(WE(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new x(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Kv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Kv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Fi.DashCase|Fi.Important)?n.style.setProperty(e,i,r&Fi.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Fi.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=jn().getGlobalEventTarget(this.doc,n),!n))throw new x(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function WE(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Qf=class extends ac{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=QE(i.id,c);for(let f of c){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=f,this.shadowRoot.appendChild(h)}let u=i.getExternalStyles?.();if(u)for(let f of u){let h=Zv(f,r);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},lc=class extends ac{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?QE(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Ko.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Yf=class extends lc{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l){let c=r+"-"+i.id;super(n,e,i,o,s,a,l,c),this.contentAttr=WP(c),this.hostAttr=qP(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Zf=class t extends ic{supportsDOMEvents=!0;static makeCurrent(){Bv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=QP();return e==null?null:YP(e)}resetBaseElement(){dc=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return oc(document.cookie,n)}},dc=null;function QP(){return dc=dc||document.head.querySelector("base"),dc?dc.getAttribute("href"):null}function YP(t){return new URL(t,document.baseURI).pathname}var KP=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),YE=["alt","control","meta","shift"],ZP={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},XP={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},KE=(()=>{class t extends sc{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>jn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),YE.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=ZP[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),YE.forEach(s=>{if(s!==r){let a=XP[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(V(G))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();async function ty(t,n,e){let i=w({rootComponent:t},JP(n,e));return TE(i)}function JP(t,n){return{platformRef:n?.platformRef,appProviders:[...rL,...t?.providers??[]],platformProviders:iL}}function eL(){Zf.makeCurrent()}function tL(){return new en}function nL(){return V_(document),document}var iL=[{provide:Xo,useValue:qv},{provide:ff,useValue:eL,multi:!0},{provide:G,useFactory:nL}];var rL=[{provide:xl,useValue:"root"},{provide:en,useFactory:tL},{provide:Kf,useClass:qf,multi:!0},{provide:Kf,useClass:KE,multi:!0},cc,Jv,Xv,{provide:lt,useExisting:cc},{provide:ts,useClass:KP},[]];var oo=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Jf=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},em=class{encodeKey(n){return ZE(n)}encodeValue(n){return ZE(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function oL(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var sL=/%(\d[a-f0-9])/gi,aL={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function ZE(t){return encodeURIComponent(t).replace(sL,(n,e)=>aL[e]??n)}function Xf(t){return`${t}`}var vr=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new em,n.fromString){if(n.fromObject)throw new x(2805,!1);this.map=oL(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Xf):[Xf(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Xf(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Xf(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function lL(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function XE(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function JE(t){return typeof Blob<"u"&&t instanceof Blob}function eS(t){return typeof FormData<"u"&&t instanceof FormData}function cL(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var tS="Content-Type",nS="Accept",iS="text/plain",rS="application/json",dL=`${rS}, ${iS}, */*`,aa=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(lL(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new x(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new oo,this.context??=new Jf,!this.params)this.params=new vr,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),l=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+l+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||XE(this.body)||JE(this.body)||eS(this.body)||cL(this.body)?this.body:this.body instanceof vr?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||eS(this.body)?null:JE(this.body)?this.body.type||null:XE(this.body)?null:typeof this.body=="string"?iS:this.body instanceof vr?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?rS:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer||this.referrer,h=n.integrity||this.integrity,m=n.referrerPolicy||this.referrerPolicy,_=n.transferCache??this.transferCache,D=n.timeout??this.timeout,E=n.body!==void 0?n.body:this.body,M=n.withCredentials??this.withCredentials,Z=n.reportProgress??this.reportProgress,Te=n.headers||this.headers,Ee=n.params||this.params,Xt=n.context??this.context;return n.setHeaders!==void 0&&(Te=Object.keys(n.setHeaders).reduce((pt,nt)=>pt.set(nt,n.setHeaders[nt]),Te)),n.setParams&&(Ee=Object.keys(n.setParams).reduce((pt,nt)=>pt.set(nt,n.setParams[nt]),Ee)),new t(e,i,E,{params:Ee,headers:Te,context:Xt,reportProgress:Z,responseType:r,withCredentials:M,transferCache:_,keepalive:o,cache:a,priority:s,timeout:D,mode:l,redirect:c,credentials:u,referrer:f,integrity:h,referrerPolicy:m})}},is=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(is||{}),ca=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new oo,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},tm=class t extends ca{constructor(n={}){super(n)}type=is.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},uc=class t extends ca{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=is.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},la=class extends ca{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},uL=200,fL=204;var mL=new b("");var hL=/^\)\]\}',?\n/;var iy=(()=>{class t{xhrFactory;tracingService=d(li,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new x(-2800,!1);let i=this.xhrFactory;return q(null).pipe(It(()=>new de(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((E,M)=>s.setRequestHeader(E,M.join(","))),e.headers.has(nS)||s.setRequestHeader(nS,dL),!e.headers.has(tS)){let E=e.detectContentTypeHeader();E!==null&&s.setRequestHeader(tS,E)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let E=e.responseType.toLowerCase();s.responseType=E!=="json"?E:"text"}let a=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let E=s.statusText||"OK",M=new oo(s.getAllResponseHeaders()),Z=s.responseURL||e.url;return l=new tm({headers:M,status:s.status,statusText:E,url:Z}),l},u=this.maybePropagateTrace(()=>{let{headers:E,status:M,statusText:Z,url:Te}=c(),Ee=null;M!==fL&&(Ee=typeof s.response>"u"?s.responseText:s.response),M===0&&(M=Ee?uL:0);let Xt=M>=200&&M<300;if(e.responseType==="json"&&typeof Ee=="string"){let pt=Ee;Ee=Ee.replace(hL,"");try{Ee=Ee!==""?JSON.parse(Ee):null}catch(nt){Ee=pt,Xt&&(Xt=!1,Ee={error:nt,text:Ee})}}Xt?(o.next(new uc({body:Ee,headers:E,status:M,statusText:Z,url:Te||void 0})),o.complete()):o.error(new la({error:Ee,headers:E,status:M,statusText:Z,url:Te||void 0}))}),f=this.maybePropagateTrace(E=>{let{url:M}=c(),Z=new la({error:E,status:s.status||0,statusText:s.statusText||"Unknown Error",url:M||void 0});o.error(Z)}),h=f;e.timeout&&(h=this.maybePropagateTrace(E=>{let{url:M}=c(),Z=new la({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:M||void 0});o.error(Z)}));let m=!1,_=this.maybePropagateTrace(E=>{m||(o.next(c()),m=!0);let M={type:is.DownloadProgress,loaded:E.loaded};E.lengthComputable&&(M.total=E.total),e.responseType==="text"&&s.responseText&&(M.partialText=s.responseText),o.next(M)}),D=this.maybePropagateTrace(E=>{let M={type:is.UploadProgress,loaded:E.loaded};E.lengthComputable&&(M.total=E.total),o.next(M)});return s.addEventListener("load",u),s.addEventListener("error",f),s.addEventListener("timeout",h),s.addEventListener("abort",f),e.reportProgress&&(s.addEventListener("progress",_),a!==null&&s.upload&&s.upload.addEventListener("progress",D)),s.send(a),o.next({type:is.Sent}),()=>{s.removeEventListener("error",f),s.removeEventListener("abort",f),s.removeEventListener("load",u),s.removeEventListener("timeout",h),e.reportProgress&&(s.removeEventListener("progress",_),a!==null&&s.upload&&s.upload.removeEventListener("progress",D)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||t)(V(ts))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function pL(t,n){return n(t)}function gL(t,n,e){return(i,r)=>yt(e,()=>n(i,o=>t(o,r)))}var ry=new b("",{factory:()=>[]}),oS=new b(""),sS=new b("",{factory:()=>!0});var oy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(iy),r},providedIn:"root"})}return t})();var nm=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Al);contributeToStability=d(sS);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(ry),...this.injector.get(oS,[])]));this.chain=i.reduceRight((r,o)=>gL(r,o,this.injector),pL)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(No(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(V(oy),V(ze))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),sy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(nm),r},providedIn:"root"})}return t})();function ny(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var ji=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof aa)o=e;else{let l;r.headers instanceof oo?l=r.headers:l=new oo(r.headers);let c;r.params&&(r.params instanceof vr?c=r.params:c=new vr({fromObject:r.params})),o=new aa(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=q(o).pipe($r(l=>this.handler.handle(l)));if(e instanceof aa||r.observe==="events")return s;let a=s.pipe(Ie(l=>l instanceof uc));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ee(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new x(2806,!1);return l.body}));case"blob":return a.pipe(ee(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new x(2807,!1);return l.body}));case"text":return a.pipe(ee(l=>{if(l.body!==null&&typeof l.body!="string")throw new x(2808,!1);return l.body}));default:return a.pipe(ee(l=>l.body))}case"response":return a;default:throw new x(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new vr().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,ny(r,i))}post(e,i,r={}){return this.request("POST",e,ny(r,i))}put(e,i,r={}){return this.request("PUT",e,ny(r,i))}static \u0275fac=function(i){return new(i||t)(V(sy))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _L=new b("",{factory:()=>!0}),vL="XSRF-TOKEN",yL=new b("",{factory:()=>vL}),bL="X-XSRF-TOKEN",wL=new b("",{factory:()=>bL}),CL=(()=>{class t{cookieName=d(yL);doc=d(G);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=oc(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),aS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(CL),r},providedIn:"root"})}return t})();function DL(t,n){if(!d(_L)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(_r).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(aS).getToken(),i=d(wL);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var ay=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t})(ay||{});function xL(t,n){return{\u0275kind:t,\u0275providers:n}}function ly(...t){let n=[ji,nm,{provide:sy,useExisting:nm},{provide:oy,useFactory:()=>d(mL,{optional:!0})??d(iy)},{provide:ry,useValue:DL,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return Ti(n)}function cy(t){return xL(ay.Interceptors,t.map(n=>({provide:ry,useValue:n,multi:!0})))}var lS=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(V(G))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var fc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=V(EL),r},providedIn:"root"})}return t})(),EL=(()=>{class t extends fc{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case bt.NONE:return i;case bt.HTML:return ur(i,"HTML")?Bn(i):q_(this._doc,String(i)).toString();case bt.STYLE:return ur(i,"Style")?Bn(i):i;case bt.SCRIPT:if(ur(i,"Script"))return Bn(i);throw new x(5200,!1);case bt.URL:return ur(i,"URL")?Bn(i):Gl(String(i));case bt.RESOURCE_URL:if(ur(i,"ResourceURL"))return Bn(i);throw new x(5201,!1);default:throw new x(5202,!1)}}bypassSecurityTrustHtml(e){return U_(e)}bypassSecurityTrustStyle(e){return H_(e)}bypassSecurityTrustScript(e){return $_(e)}bypassSecurityTrustUrl(e){return G_(e)}bypassSecurityTrustResourceUrl(e){return W_(e)}static \u0275fac=function(i){return new(i||t)(V(G))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ye="primary",Ec=Symbol("RouteTitle"),hy=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function os(t){return new hy(t)}function dy(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function gS(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return dy(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!dy(o,t.slice(0,o.length),a)||!dy(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function cm(t){return new Promise((n,e)=>{t.pipe(nr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function SL(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!zi(t[e],n[e]))return!1;return!0}function zi(t,n){let e=t?py(t):void 0,i=n?py(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!_S(t[r],n[r]))return!1;return!0}function py(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function _S(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function IL(t){return t.length>0?t[t.length-1]:null}function as(t){return Ro(t)?t:ro(t)?Ge(Promise.resolve(t)):q(t)}function vS(t){return Ro(t)?cm(t):Promise.resolve(t)}var kL={exact:bS,subset:wS},yS={exact:ML,subset:TL,ignored:()=>!0},My={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},_c={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function Ty(t,n,e){let i=t instanceof ln?t:n.parseUrl(t);return sn(()=>gy(n.lastSuccessfulNavigation()?.finalUrl??new ln,i,w(w({},_c),e)))}function gy(t,n,e){return kL[e.paths](t.root,n.root,e.matrixParams)&&yS[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function ML(t,n){return zi(t,n)}function bS(t,n,e){if(!rs(t.segments,n.segments)||!sm(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!bS(t.children[i],n.children[i],e))return!1;return!0}function TL(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>_S(t[e],n[e]))}function wS(t,n,e){return CS(t,n,n.segments,e)}function CS(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!rs(r,e)||n.hasChildren()||!sm(r,e,i))}else if(t.segments.length===e.length){if(!rs(t.segments,e)||!sm(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!wS(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!rs(t.segments,r)||!sm(t.segments,r,i)||!t.children[ye]?!1:CS(t.children[ye],n,o,i)}}function sm(t,n,e){return n.every((i,r)=>yS[e](t[r].parameters,i.parameters))}var ln=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Be([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=os(this.queryParams),this._queryParamMap}toString(){return OL.serialize(this)}},Be=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return am(this)}},so=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=os(this.parameters),this._parameterMap}toString(){return xS(this)}};function AL(t,n){return rs(t,n)&&t.every((e,i)=>zi(e.parameters,n[i].parameters))}function rs(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function RL(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===ye&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==ye&&(e=e.concat(n(r,i)))}),e}var co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new br,providedIn:"root"})}return t})(),br=class{parse(n){let e=new vy(n);return new ln(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${mc(n.root,!0)}`,i=PL(n.queryParams),r=typeof n.fragment=="string"?`#${NL(n.fragment)}`:"";return`${e}${i}${r}`}},OL=new br;function am(t){return t.segments.map(n=>xS(n)).join("/")}function mc(t,n){if(!t.hasChildren())return am(t);if(n){let e=t.children[ye]?mc(t.children[ye],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==ye&&i.push(`${r}:${mc(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=RL(t,(i,r)=>r===ye?[mc(t.children[ye],!1)]:[`${r}:${mc(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[ye]!=null?`${am(t)}/${e[0]}`:`${am(t)}/(${e.join("//")})`}}function DS(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function rm(t){return DS(t).replace(/%3B/gi,";")}function NL(t){return encodeURI(t)}function _y(t){return DS(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function lm(t){return decodeURIComponent(t)}function dS(t){return lm(t.replace(/\+/g,"%20"))}function xS(t){return`${_y(t.path)}${FL(t.parameters)}`}function FL(t){return Object.entries(t).map(([n,e])=>`;${_y(n)}=${_y(e)}`).join("")}function PL(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${rm(e)}=${rm(r)}`).join("&"):`${rm(e)}=${rm(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var LL=/^[^\/()?;#]+/;function uy(t){let n=t.match(LL);return n?n[0]:""}var BL=/^[^\/()?;=#]+/;function VL(t){let n=t.match(BL);return n?n[0]:""}var jL=/^[^=?&#]+/;function zL(t){let n=t.match(jL);return n?n[0]:""}var UL=/^[^&#]+/;function HL(t){let n=t.match(UL);return n?n[0]:""}var vy=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Be([],{}):new Be([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[ye]=new Be(e,i)),r}parseSegment(){let n=uy(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(n),new so(lm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=VL(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=uy(this.remaining);r&&(i=r,this.capture(i))}n[lm(e)]=lm(i)}parseQueryParam(n){let e=zL(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=HL(this.remaining);s&&(i=s,this.capture(i))}let r=dS(e),o=dS(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=uy(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new x(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=ye);let a=this.parseChildren(e+1);i[s??ye]=Object.keys(a).length===1&&a[ye]?a[ye]:new Be([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new x(4011,!1)}};function ES(t){return t.segments.length>0?new Be([],{[ye]:t}):t}function SS(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=SS(r);if(i===ye&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Be(t.segments,n);return $L(e)}function $L(t){if(t.numberOfChildren===1&&t.children[ye]){let n=t.children[ye];return new Be(t.segments.concat(n.segments),n.children)}return t}function ao(t){return t instanceof ln}function IS(t,n,e=null,i=null,r=new br){let o=kS(t);return MS(o,n,e,i,r)}function kS(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new Be(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=ES(i);return n??r}function MS(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return fy(o,o,o,e,i,r);let s=GL(n);if(s.toRoot())return fy(o,o,new Be([],{}),e,i,r);let a=WL(s,o,t),l=a.processChildren?pc(a.segmentGroup,a.index,s.commands):AS(a.segmentGroup,a.index,s.commands);return fy(o,a.segmentGroup,l,e,i,r)}function dm(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function vc(t){return typeof t=="object"&&t!=null&&t.outlets}function uS(t,n,e){t||="\u0275";let i=new ln;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function fy(t,n,e,i,r,o){let s={};for(let[c,u]of Object.entries(i??{}))s[c]=Array.isArray(u)?u.map(f=>uS(c,f,o)):uS(c,u,o);let a;t===n?a=e:a=TS(t,n,e);let l=ES(SS(a));return new ln(l,s,r)}function TS(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=TS(o,n,e)}),new Be(t.segments,i)}var um=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&dm(i[0]))throw new x(4003,!1);let r=i.find(vc);if(r&&r!==IL(i))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function GL(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new um(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new um(e,n,i)}var ua=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function WL(t,n,e){if(t.isAbsolute)return new ua(n,!0,0);if(!e)return new ua(n,!1,NaN);if(e.parent===null)return new ua(e,!0,0);let i=dm(t.commands[0])?0:1,r=e.segments.length-1+i;return qL(e,r,t.numberOfDoubleDots)}function qL(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new x(4005,!1);r=i.segments.length}return new ua(i,!1,r-o)}function QL(t){return vc(t[0])?t[0].outlets:{[ye]:t}}function AS(t,n,e){if(t??=new Be([],{}),t.segments.length===0&&t.hasChildren())return pc(t,n,e);let i=YL(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Be(t.segments.slice(0,i.pathIndex),{});return o.children[ye]=new Be(t.segments.slice(i.pathIndex),t.children),pc(o,0,r)}else return i.match&&r.length===0?new Be(t.segments,{}):i.match&&!t.hasChildren()?yy(t,n,e):i.match?pc(t,0,r):yy(t,n,e)}function pc(t,n,e){if(e.length===0)return new Be(t.segments,{});{let i=QL(e),r={};if(Object.keys(i).some(o=>o!==ye)&&t.children[ye]&&t.numberOfChildren===1&&t.children[ye].segments.length===0){let o=pc(t.children[ye],n,e);return new Be(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=AS(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Be(t.segments,r)}}function YL(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(vc(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!mS(l,c,s))return o;i+=2}else{if(!mS(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function yy(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(vc(o)){let l=KL(o.outlets);return new Be(i,l)}if(r===0&&dm(e[0])){let l=t.segments[n];i.push(new so(l.path,fS(e[0]))),r++;continue}let s=vc(o)?o.outlets[ye]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&dm(a)?(i.push(new so(s,fS(a))),r+=2):(i.push(new so(s,{})),r++)}return new Be(i,{})}function KL(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=yy(new Be([],{}),0,i))}),n}function fS(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function mS(t,n,e){return t==e.path&&zi(n,e.parameters)}var fa="imperative",Ct=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Ct||{}),In=class{id;url;constructor(n,e){this.id=n,this.url=e}},lo=class extends In{type=Ct.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},kn=class extends In{urlAfterRedirects;type=Ct.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Qt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Qt||{}),ha=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ha||{}),zn=class extends In{reason;code;type=Ct.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function RS(t){return t instanceof zn&&(t.code===Qt.Redirect||t.code===Qt.SupersededByNewNavigation)}var Ui=class extends In{reason;code;type=Ct.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ss=class extends In{error;target;type=Ct.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},yc=class extends In{urlAfterRedirects;state;type=Ct.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},fm=class extends In{urlAfterRedirects;state;type=Ct.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},mm=class extends In{urlAfterRedirects;state;shouldActivate;type=Ct.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},hm=class extends In{urlAfterRedirects;state;type=Ct.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},pm=class extends In{urlAfterRedirects;state;type=Ct.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},gm=class{route;type=Ct.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},_m=class{route;type=Ct.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},vm=class{snapshot;type=Ct.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ym=class{snapshot;type=Ct.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},bm=class{snapshot;type=Ct.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},wm=class{snapshot;type=Ct.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},pa=class{routerEvent;position;anchor;scrollBehavior;type=Ct.Scroll;constructor(n,e,i,r){this.routerEvent=n,this.position=e,this.anchor=i,this.scrollBehavior=r}toString(){let n=this.position?`${this.position[0]}, ${this.position[1]}`:null;return`Scroll(anchor: '${this.anchor}', position: '${n}')`}},ga=class{},bc=class{},_a=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function ZL(t){return!(t instanceof ga)&&!(t instanceof _a)&&!(t instanceof bc)}var Cm=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new ls(this.rootInjector)}},ls=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Cm(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(V(ze))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Dm=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=by(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=by(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=wy(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return wy(n,this._root).map(e=>e.value)}};function by(t,n){if(t===n.value)return n;for(let e of n.children){let i=by(t,e);if(i)return i}return null}function wy(t,n){if(t===n.value)return[n];for(let e of n.children){let i=wy(t,e);if(i.length)return i.unshift(n),i}return[]}var Sn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function da(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var wc=class extends Dm{snapshot;constructor(n,e){super(n),this.snapshot=e,Ry(this,n)}toString(){return this.snapshot.toString()}};function OS(t,n){let e=XL(t,n),i=new qe([new so("",{})]),r=new qe({}),o=new qe({}),s=new qe({}),a=new qe(""),l=new Hi(i,r,s,a,o,ye,t,e.root);return l.snapshot=e.root,new wc(new Sn(l,[]),e)}function XL(t,n){let e={},i={},r={},s=new va([],e,r,"",i,ye,t,null,{},n);return new Cc("",new Sn(s,[]))}var Hi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(ee(c=>c[Ec]))??q(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ee(n=>os(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ee(n=>os(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Ay(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:w(w({},n.params),t.params),data:w(w({},n.data),t.data),resolve:w(w(w(w({},t.data),n.data),r?.data),t._resolvedData)}:i={params:w({},t.params),data:w({},t.data),resolve:w(w({},t.data),t._resolvedData??{})},r&&FS(r)&&(i.resolve[Ec]=r.title),i}var va=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Ec]}constructor(n,e,i,r,o,s,a,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=os(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=os(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},Cc=class extends Dm{url;constructor(n,e){super(e),this.url=n,Ry(this,e)}toString(){return NS(this._root)}};function Ry(t,n){n.value._routerState=t,n.children.forEach(e=>Ry(t,e))}function NS(t){let n=t.children.length>0?` { ${t.children.map(NS).join(", ")} } `:"";return`${t.value}${n}`}function my(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,zi(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),zi(n.params,e.params)||t.paramsSubject.next(e.params),SL(n.url,e.url)||t.urlSubject.next(e.url),zi(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Cy(t,n){let e=zi(t.params,n.params)&&AL(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Cy(t.parent,n.parent))}function FS(t){return typeof t.title=="string"||t.title===null}var PS=new b(""),cs=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ye;activateEvents=new Q;deactivateEvents=new Q;attachEvents=new Q;detachEvents=new Q;routerOutletData=kE();parentContexts=d(ls);location=d(ot);changeDetector=d(Ce);inputBinder=d(Sc,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new Dy(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Pe]})}return t})(),Dy=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Hi?this.route:n===ls?this.childContexts:n===PS?this.outletData:this.parent.get(n,e)}},Sc=new b(""),Oy=(()=>{class t{outletDataSubscriptions=new Map;bindActivatedRouteToOutletComponent(e){this.unsubscribeFromRouteData(e),this.subscribeToRouteData(e)}unsubscribeFromRouteData(e){this.outletDataSubscriptions.get(e)?.unsubscribe(),this.outletDataSubscriptions.delete(e)}subscribeToRouteData(e){let{activatedRoute:i}=e,r=hn([i.queryParams,i.params,i.data]).pipe(It(([o,s,a],l)=>(a=w(w(w({},o),s),a),l===0?q(a):Promise.resolve(a)))).subscribe(o=>{if(!e.isActivated||!e.activatedComponentRef||e.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(e);return}let s=OE(i.component);if(!s){this.unsubscribeFromRouteData(e);return}for(let{templateName:a}of s.inputs)e.activatedComponentRef.setInput(a,o[a])});this.outletDataSubscriptions.set(e,r)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Ny=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&F(0,"router-outlet")},dependencies:[cs],encapsulation:2})}return t})();function Fy(t){let n=t.children&&t.children.map(Fy),e=n?oe(w({},t),{children:n}):w({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ye&&(e.component=Ny),e}function JL(t,n,e){let i=Dc(t,n._root,e?e._root:void 0);return new wc(i,n)}function Dc(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=e2(t,n,e);return new Sn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>Dc(t,a)),s}}let i=t2(n.value),r=n.children.map(o=>Dc(t,o));return new Sn(i,r)}}function e2(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return Dc(t,i,r);return Dc(t,i)})}function t2(t){return new Hi(new qe(t.url),new qe(t.params),new qe(t.queryParams),new qe(t.fragment),new qe(t.data),t.outlet,t.component,t)}var ya=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},LS="ngNavigationCancelingError";function xm(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=ao(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=BS(!1,Qt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function BS(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[LS]=!0,e.cancellationCode=n,e}function n2(t){return VS(t)&&ao(t.url)}function VS(t){return!!t&&t[LS]}var xy=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),my(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=da(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=da(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=da(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=da(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new wm(o.value.snapshot))}),n.children.length&&this.forwardEvent(new ym(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(my(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),my(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Em=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ma=class{component;route;constructor(n,e){this.component=n,this.route=e}};function i2(t,n,e){let i=t._root,r=n?n._root:null;return hc(i,r,e,[i.value])}function r2(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function wa(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Zp(t)?t:n.get(t):i}function hc(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=da(n);return t.children.forEach(s=>{o2(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>gc(a,e.getContext(s),r)),r}function o2(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=s2(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Em(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?hc(t,n,a?a.children:null,i,r):hc(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ma(a.outlet.component,s))}else s&&gc(n,a,r),r.canActivateChecks.push(new Em(i)),o.component?hc(t,null,a?a.children:null,i,r):hc(t,null,e,i,r);return r}function s2(t,n,e){if(typeof e=="function")return yt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!rs(t.url,n.url);case"pathParamsOrQueryParamsChange":return!rs(t.url,n.url)||!zi(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Cy(t,n)||!zi(t.queryParams,n.queryParams);default:return!Cy(t,n)}}function gc(t,n,e){let i=da(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?gc(s,n.children.getContext(o),e):gc(s,null,e):gc(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new ma(n.outlet.component,r)):e.canDeactivateChecks.push(new ma(null,r)):e.canDeactivateChecks.push(new ma(null,r))}function Ic(t){return typeof t=="function"}function a2(t){return typeof t=="boolean"}function l2(t){return t&&Ic(t.canLoad)}function c2(t){return t&&Ic(t.canActivate)}function d2(t){return t&&Ic(t.canActivateChild)}function u2(t){return t&&Ic(t.canDeactivate)}function f2(t){return t&&Ic(t.canMatch)}function jS(t){return t instanceof Oo||t?.name==="EmptyError"}var om=Symbol("INITIAL_VALUE");function ba(){return It(t=>hn(t.map(n=>n.pipe(rt(1),St(om)))).pipe(ee(n=>{for(let e of n)if(e!==!0){if(e===om)return om;if(e===!1||m2(e))return e}return!0}),Ie(n=>n!==om),rt(1)))}function m2(t){return ao(t)||t instanceof ya}function zS(t){return t.aborted?q(void 0).pipe(rt(1)):new de(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function US(t){return ue(zS(t))}function h2(t){return Ft(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?q(oe(w({},n),{guardsResult:!0})):p2(o,e,i).pipe(Ft(s=>s&&a2(s)?g2(e,r,t):q(s)),ee(s=>oe(w({},n),{guardsResult:s})))})}function p2(t,n,e){return Ge(t).pipe(Ft(i=>w2(i.component,i.route,e,n)),nr(i=>i!==!0,!0))}function g2(t,n,e){return Ge(n).pipe($r(i=>Ur(v2(i.route.parent,e),_2(i.route,e),b2(t,i.path),y2(t,i.route))),nr(i=>i!==!0,!0))}function _2(t,n){return t!==null&&n&&n(new bm(t)),q(!0)}function v2(t,n){return t!==null&&n&&n(new vm(t)),q(!0)}function y2(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return q(!0);let i=e.map(r=>Zn(()=>{let o=n._environmentInjector,s=wa(r,o),a=c2(s)?s.canActivate(n,t):yt(o,()=>s(n,t));return as(a).pipe(nr())}));return q(i).pipe(ba())}function b2(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>r2(o)).filter(o=>o!==null).map(o=>Zn(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=wa(a,l),u=d2(c)?c.canActivateChild(e,t):yt(l,()=>c(e,t));return as(u).pipe(nr())});return q(s).pipe(ba())}));return q(r).pipe(ba())}function w2(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return q(!0);let o=r.map(s=>{let a=n._environmentInjector,l=wa(s,a),c=u2(l)?l.canDeactivate(t,n,e,i):yt(a,()=>l(t,n,e,i));return as(c).pipe(nr())});return q(o).pipe(ba())}function C2(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return q(!0);let s=o.map(a=>{let l=wa(a,t),c=l2(l)?l.canLoad(n,e):yt(t,()=>l(n,e)),u=as(c);return r?u.pipe(US(r)):u});return q(s).pipe(ba(),HS(i))}function HS(t){return Ip(st(n=>{if(typeof n!="boolean")throw xm(t,n)}),ee(n=>n===!0))}function D2(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return q(!0);let a=s.map(l=>{let c=wa(l,t),u=f2(c)?c.canMatch(n,e,r):yt(t,()=>c(n,e,r));return as(u).pipe(US(o))});return q(a).pipe(ba(),HS(i))}var yr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},xc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function x2(t){throw new x(4e3,!1)}function E2(t){throw BS(!1,Qt.GuardRejected)}var Ey=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[ye])throw x2(`${n.redirectTo}`);r=r.children[ye]}}async applyRedirectCommands(n,e,i,r,o){let s=await S2(e,r,o);if(s instanceof ln)throw new xc(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new xc(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new ln(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new Be(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new x(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function S2(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return cm(as(yt(e,()=>i(n))))}function I2(t,n){return t.providers&&!t._injector&&(t._injector=ra(t.providers,n,`Route: ${t.path}`)),t._injector??n}function fi(t){return t.outlet||ye}function k2(t,n){let e=t.filter(i=>fi(i)===n);return e.push(...t.filter(i=>fi(i)!==n)),e}var Sy={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function $S(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function M2(t,n,e,i,r,o,s){let a=GS(t,n,e);if(!a.matched)return q(a);let l=$S(o(a));return i=I2(n,i),D2(i,n,e,r,l,s).pipe(ee(c=>c===!0?a:w({},Sy)))}function GS(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?w({},Sy):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||gS)(e,t,n);if(!r)return w({},Sy);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?w(w({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function hS(t,n,e,i,r){return e.length>0&&R2(t,e,i,r)?{segmentGroup:new Be(n,A2(i,new Be(e,t.children))),slicedSegments:[]}:e.length===0&&O2(t,e,i)?{segmentGroup:new Be(t.segments,T2(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Be(t.segments,t.children),slicedSegments:e}}function T2(t,n,e,i){let r={};for(let o of e)if(Im(t,n,o)&&!i[fi(o)]){let s=new Be([],{});r[fi(o)]=s}return w(w({},i),r)}function A2(t,n){let e={};e[ye]=n;for(let i of t)if(i.path===""&&fi(i)!==ye){let r=new Be([],{});e[fi(i)]=r}return e}function R2(t,n,e,i){return e.some(r=>!Im(t,n,r)||!(fi(r)!==ye)?!1:!(i!==void 0&&fi(r)===i))}function O2(t,n,e){return e.some(i=>Im(t,n,i))}function Im(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function N2(t,n,e){return n.length===0&&!t.children[e]}var Iy=class{};async function F2(t,n,e,i,r,o,s="emptyOnly",a){return new ky(t,n,e,i,r,s,o,a).recognize()}var P2=31,ky=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new Ey(this.urlSerializer,this.urlTree)}noMatchError(n){return new x(4002,`'${n.segmentGroup}'`)}async recognize(){let n=hS(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Sn(i,e),o=new Cc("",r),s=IS(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new va([],Object.freeze({}),Object.freeze(w({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),ye,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ye,e),rootSnapshot:e}}catch(i){if(i instanceof xc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof yr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Sn?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],u=k2(e,l),f=await this.processSegmentGroup(n,u,c,l,r);s.push(...f)}let a=WS(s);return L2(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof yr||jS(c))continue;throw c}if(N2(i,r,o))return new Iy;throw new yr(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(fi(i)!==s&&(s===ye||!Im(r,o,i)))throw new yr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new yr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:h}=GS(e,r,o);if(!l)throw new yr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>P2&&(this.allowRedirects=!1));let m=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let _=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,$S(m),n),D=await this.applyRedirects.lineralizeSegments(r,_);return this.processSegment(n,i,e,D.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new va(i,r,Object.freeze(w({},this.urlTree.queryParams)),this.urlTree.fragment,V2(e),fi(e),e.component??e._loadedComponent??null,e,j2(e),n),a=Ay(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Te=>this.createSnapshot(n,i,Te.consumedSegments,Te.parameters,s),l=await cm(M2(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new yr(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:m}=l,_=this.createSnapshot(n,i,h,f,s),{segmentGroup:D,slicedSegments:E}=hS(e,h,m,c,o);if(E.length===0&&D.hasChildren()){let Te=await this.processChildren(u,c,D,_);return new Sn(_,Te)}if(c.length===0&&E.length===0)return new Sn(_,[]);let M=fi(i)===o,Z=await this.processSegment(u,c,D,E,M?ye:o,!0,_);return new Sn(_,Z instanceof Sn?[Z]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await cm(C2(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw E2(e)}return{routes:[],injector:n}}};function L2(t){t.sort((n,e)=>n.value.outlet===ye?-1:e.value.outlet===ye?1:n.value.outlet.localeCompare(e.value.outlet))}function B2(t){let n=t.value.routeConfig;return n&&n.path===""}function WS(t){let n=[],e=new Set;for(let i of t){if(!B2(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=WS(i.children);n.push(new Sn(i.value,r))}return n.filter(i=>!e.has(i))}function V2(t){return t.data||{}}function j2(t){return t.resolve||{}}function z2(t,n,e,i,r,o,s){return Ft(async a=>{let{state:l,tree:c}=await F2(t,n,e,i,a.extractedUrl,r,o,s);return oe(w({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function U2(t){return Ft(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return q(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of qS(a))o.add(l);let s=0;return Ge(o).pipe($r(a=>r.has(a)?H2(a,e,t):(a.data=Ay(a,a.parent,t).resolve,q(void 0))),st(()=>s++),au(1),Ft(a=>s===o.size?q(n):ut))})}function qS(t){let n=t.children.map(e=>qS(e)).flat();return[t,...n]}function H2(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!FS(i)&&(r[Ec]=i.title),Zn(()=>(t.data=Ay(t,t.parent,e).resolve,$2(r,t,n).pipe(ee(o=>(t._resolvedData=o,t.data=w(w({},t.data),o),null)))))}function $2(t,n,e){let i=py(t);if(i.length===0)return q({});let r={};return Ge(i).pipe(Ft(o=>G2(t[o],n,e).pipe(nr(),st(s=>{if(s instanceof ya)throw xm(new br,s);r[o]=s}))),au(1),ee(()=>r),Hr(o=>jS(o)?ut:ul(o)))}function G2(t,n,e){let i=n._environmentInjector,r=wa(t,i),o=r.resolve?r.resolve(n,e):yt(i,()=>r(n,e));return as(o)}function pS(t){return It(n=>{let e=t(n);return e?Ge(e).pipe(ee(()=>n)):q(n)})}var Py=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===ye);return i}getResolvedTitleForRoute(e){return e.data[Ec]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(QS),providedIn:"root"})}return t})(),QS=(()=>{class t extends Py{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(V(lS))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uo=new b("",{factory:()=>({})}),ds=new b(""),km=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(kv);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await vS(yt(e,()=>i.loadComponent())),s=await ZS(KS(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await YS(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function YS(t,n,e,i){let r=await vS(yt(e,()=>t.loadChildren())),o=await ZS(KS(r)),s;o instanceof Ef||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,u;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,u=s,l=a.get(ds,[],{optional:!0,self:!0}).flat()),{routes:l.map(Fy),injector:a,factory:u}}function W2(t){return t&&typeof t=="object"&&"default"in t}function KS(t){return W2(t)?t.default:t}async function ZS(t){return t}var Mm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(q2),providedIn:"root"})}return t})(),q2=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ly=new b(""),By=new b("");function XS(t,n,e){let i=t.get(By),r=t.get(G);if(!r.startViewTransition||i.skipNextTransition)return i.skipNextTransition=!1,new Promise(c=>setTimeout(c));let o,s=new Promise(c=>{o=c}),a=r.startViewTransition(()=>(o(),Q2(t)));a.updateCallbackDone.catch(c=>{}),a.ready.catch(c=>{}),a.finished.catch(c=>{});let{onViewTransitionCreated:l}=i;return l&&yt(t,()=>l({transition:a,from:n,to:e})),s}function Q2(t){return new Promise(n=>{Ke({read:()=>setTimeout(n)},{injector:t})})}var Y2=()=>{},Vy=new b(""),Tm=(()=>{class t{currentNavigation=X(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=X(null);events=new I;transitionAbortWithErrorSubject=new I;configLoader=d(km);environmentInjector=d(ze);destroyRef=d(Ht);urlSerializer=d(co);rootContexts=d(ls);location=d(Bi);inputBindingEnabled=d(Sc,{optional:!0})!==null;titleStrategy=d(Py);options=d(uo,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(Mm);createViewTransition=d(Ly,{optional:!0});navigationErrorHandler=d(Vy,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>q(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new gm(r)),i=r=>this.events.next(new _m(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Le(()=>{this.transitions?.next(oe(w({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new qe(null),this.transitions.pipe(Ie(i=>i!==null),It(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return q(i).pipe(It(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Qt.SupersededByNewNavigation),ut;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:l?oe(w({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new Ui(a.id,this.urlSerializer.serialize(a.rawUrl),"",ha.IgnoredSameUrlNavigation)),a.resolve(!1),ut;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return q(a).pipe(It(f=>(this.events.next(new lo(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?ut:Promise.resolve(f))),z2(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),st(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=f.urlAfterRedirects,h)),this.events.next(new bc)}),It(f=>Ge(i.routesRecognizeHandler.deferredHandle??q(void 0)).pipe(ee(()=>f))),st(()=>{let f=new yc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:f,extractedUrl:h,source:m,restoredState:_,extras:D}=a,E=new lo(f,this.urlSerializer.serialize(h),m,_);this.events.next(E);let M=OS(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=oe(w({},a),{targetSnapshot:M,urlAfterRedirects:h,extras:oe(w({},D),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Z=>(Z.finalUrl=h,Z)),q(i)}else return this.events.next(new Ui(a.id,this.urlSerializer.serialize(a.extractedUrl),"",ha.IgnoredByUrlHandlingStrategy)),a.resolve(!1),ut}),ee(a=>{let l=new fm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(l),this.currentTransition=i=oe(w({},a),{guards:i2(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),h2(a=>this.events.next(a)),It(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw xm(this.urlSerializer,a.guardsResult);let l=new mm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(l),!s())return ut;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",Qt.GuardRejected),ut;if(a.guards.canActivateChecks.length===0)return q(a);let c=new hm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(c),!s())return ut;let u=!1;return q(a).pipe(U2(this.paramsInheritanceStrategy),st({next:()=>{u=!0;let f=new pm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)},complete:()=>{u||this.cancelNavigationTransition(a,"",Qt.NoDataFromResolver)}}))}),pS(a=>{let l=u=>{let f=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let h=u._environmentInjector;f.push(this.configLoader.loadComponent(h,u.routeConfig).then(m=>{u.component=m}))}for(let h of u.children)f.push(...l(h));return f},c=l(a.targetSnapshot.root);return c.length===0?q(a):Ge(Promise.all(c).then(()=>a))}),pS(()=>this.afterPreactivation()),It(()=>{let{currentSnapshot:a,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,a.root,l.root);return c?Ge(c).pipe(ee(()=>i)):q(i)}),rt(1),It(a=>{let l=JL(e.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=oe(w({},a),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new ga);let c=i.beforeActivateHandler.deferredHandle;return c?Ge(c.then(()=>a)):q(a)}),st(a=>{new xy(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(l=>(l.abort=Y2,l)),this.lastSuccessfulNavigation.set(Le(this.currentNavigation)),this.events.next(new kn(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),ue(zS(o.signal).pipe(Ie(()=>!r&&!i.targetRouterState),st(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Qt.Aborted)}))),st({complete:()=>{r=!0}}),ue(this.transitionAbortWithErrorSubject.pipe(st(a=>{throw a}))),No(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Qt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Hr(a=>{if(r=!0,this.destroyed)return i.resolve(!1),ut;if(VS(a))this.events.next(new zn(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),n2(a)?this.events.next(new _a(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ss(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let c=yt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof ya){let{message:u,cancellationCode:f}=xm(this.urlSerializer,c);this.events.next(new zn(i.id,this.urlSerializer.serialize(i.extractedUrl),u,f)),this.events.next(new _a(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),a}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return ut}))}))}cancelNavigationTransition(e,i,r){let o=new zn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Le(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function K2(t){return t!==fa}var JS=new b("");var eI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(Z2),providedIn:"root"})}return t})(),Sm=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},Z2=(()=>{class t extends Sm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Am=(()=>{class t{urlSerializer=d(co);options=d(uo,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Bi);urlHandlingStrategy=d(Mm);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new ln;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof ln?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=OS(null,d(ze));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(X2),providedIn:"root"})}return t})(),X2=(()=>{class t extends Am{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof lo?this.updateStateMemento():e instanceof Ui?this.commitTransition(i):e instanceof yc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ga?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof zn&&!RS(e)?this.restoreHistory(i):e instanceof ss?this.restoreHistory(i,!0):e instanceof kn&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=w(w({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=w(w({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?w({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):w({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Rm(t,n){t.events.pipe(Ie(e=>e instanceof kn||e instanceof zn||e instanceof ss||e instanceof Ui),ee(e=>e instanceof kn||e instanceof Ui?0:(e instanceof zn?e.code===Qt.Redirect||e.code===Qt.SupersededByNewNavigation:!1)?2:1),Ie(e=>e!==2),rt(1)).subscribe(()=>{n()})}var mt=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Sf);stateManager=d(Am);options=d(uo,{optional:!0})||{};pendingTasks=d(ar);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(Tm);urlSerializer=d(co);location=d(Bi);urlHandlingStrategy=d(Mm);injector=d(ze);_events=new I;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(eI);injectorCleanup=d(JS,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(ds,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Sc,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new Se;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Le(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof zn&&i.code!==Qt.Redirect&&i.code!==Qt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof kn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof _a){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=w({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||K2(r.source)},s);this.scheduleNavigation(a,fa,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}ZL(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),fa,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=oe(w({},o),{browserUrl:e})),r){let c=w({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(a);this.scheduleNavigation(l,i,s,o).catch(c=>{this.disposed||this.injector.get(En)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Le(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Fy),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=w(w({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=kS(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return MS(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=ao(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,fa,null,i)}navigate(e,i={skipLocationChange:!1}){return J2(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Xn(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=w({},My):i===!1?r=w({},_c):r=w(w({},_c),i),ao(e))return gy(this.currentUrlTree,e,r);let o=this.parseUrl(e);return gy(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,l,c;s?(a=s.resolve,l=s.reject,c=s.promise):c=new Promise((f,h)=>{a=f,l=h});let u=this.pendingTasks.add();return Rm(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function J2(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new x(4008,!1)}var eB=(()=>{class t{router=d(mt);stateManager=d(Am);fragment=X("");queryParams=X({});path=X("");serializer=d(co);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof kn&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new ln(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Un=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new _n("href"),{optional:!0});reactiveHref=Mv(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Le(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Le(this._target)}_target=X(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Le(this._queryParams)}_queryParams=X(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Le(this._fragment)}_fragment=X(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Le(this._queryParamsHandling)}_queryParamsHandling=X(void 0);set state(e){this._state.set(e)}get state(){return Le(this._state)}_state=X(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Le(this._info)}_info=X(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Le(this._relativeTo)}_relativeTo=X(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Le(this._preserveFragment)}_preserveFragment=X(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Le(this._skipLocationChange)}_skipLocationChange=X(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Le(this._replaceUrl)}_replaceUrl=X(!1);isAnchorElement;onChanges=new I;applicationErrorHandler=d(En);options=d(uo,{optional:!0});reactiveRouterState=d(eB);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let l=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=X(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(ao(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(a,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=sn(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:ao(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Le(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(R(mt),R(Hi),$l("tabindex"),R($e),R(O),R(di))};static \u0275dir=S({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&A("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&te("href",r.reactiveHref(),Q_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",j],skipLocationChange:[2,"skipLocationChange","skipLocationChange",j],replaceUrl:[2,"replaceUrl","replaceUrl",j],routerLink:"routerLink"},features:[Pe]})}return t})(),zy=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new Q;link=d(Un,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(s=>{s instanceof kn&&this.update()})}ngAfterContentInit(){q(this.links.changes,q(null)).pipe(Si()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=Ge(e).pipe(Si()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=tB(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?w({},My):w({},_c);return r=>{let o=r.urlTree;return o?Le(Ty(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(R(mt),R(O),R($e),R(Ce))};static \u0275dir=S({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&wt(o,Un,5),i&2){let s;H(s=$())&&(r.links=s)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Pe]})}return t})();function tB(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var kc=class{};var tI=(()=>{class t{router;injector;preloadingStrategy;loader;subscription;constructor(e,i,r,o){this.router=e,this.injector=i,this.preloadingStrategy=r,this.loader=o}setUpPreloading(){this.subscription=this.router.events.pipe(Ie(e=>e instanceof kn),$r(()=>this.preload())).subscribe(()=>{})}preload(){return this.processRoutes(this.injector,this.router.config)}ngOnDestroy(){this.subscription?.unsubscribe()}processRoutes(e,i){let r=[];for(let o of i){o.providers&&!o._injector&&(o._injector=ra(o.providers,e,""));let s=o._injector??e;o._loadedNgModuleFactory&&!o._loadedInjector&&(o._loadedInjector=o._loadedNgModuleFactory.create(s).injector);let a=o._loadedInjector??s;(o.loadChildren&&!o._loadedRoutes&&o.canLoad===void 0||o.loadComponent&&!o._loadedComponent)&&r.push(this.preloadConfig(s,o)),(o.children||o._loadedRoutes)&&r.push(this.processRoutes(a,o.children??o._loadedRoutes))}return Ge(r).pipe(Si())}preloadConfig(e,i){return this.preloadingStrategy.preload(i,()=>{if(e.destroyed)return q(null);let r;i.loadChildren&&i.canLoad===void 0?r=Ge(this.loader.loadChildren(e,i)):r=q(null);let o=r.pipe(Ft(s=>s===null?q(void 0):(i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,this.processRoutes(s.injector??e,s.routes))));if(i.loadComponent&&!i._loadedComponent){let s=this.loader.loadComponent(e,i);return Ge([o,s]).pipe(Si())}else return o})}static \u0275fac=function(i){return new(i||t)(V(mt),V(ze),V(kc),V(km))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),nI=new b(""),nB=(()=>{class t{options;routerEventsSubscription;scrollEventsSubscription;lastId=0;lastSource=fa;restoredId=0;store={};isHydrating=d(j_,{optional:!0})??!1;urlSerializer=d(co);zone=d(z);viewportScroller=d(Qv);transitions=d(Tm);constructor(e){this.options=e,this.options.scrollPositionRestoration||="disabled",this.options.anchorScrolling||="disabled",this.isHydrating&&d(pn).whenStable().then(()=>{this.isHydrating=!1})}init(){this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.setHistoryScrollRestoration("manual"),this.routerEventsSubscription=this.createScrollEvents(),this.scrollEventsSubscription=this.consumeScrollEvents()}createScrollEvents(){return this.transitions.events.subscribe(e=>{e instanceof lo?(this.store[this.lastId]=this.viewportScroller.getScrollPosition(),this.lastSource=e.navigationTrigger,this.restoredId=e.restoredState?e.restoredState.navigationId:0):e instanceof kn?(this.lastId=e.id,this.scheduleScrollEvent(e,this.urlSerializer.parse(e.urlAfterRedirects).fragment)):e instanceof Ui&&e.code===ha.IgnoredSameUrlNavigation&&(this.lastSource=void 0,this.restoredId=0,this.scheduleScrollEvent(e,this.urlSerializer.parse(e.url).fragment))})}consumeScrollEvents(){return this.transitions.events.subscribe(e=>{if(!(e instanceof pa)||e.scrollBehavior==="manual")return;let i={behavior:"instant"};e.position?this.options.scrollPositionRestoration==="top"?this.viewportScroller.scrollToPosition([0,0],i):this.options.scrollPositionRestoration==="enabled"&&this.viewportScroller.scrollToPosition(e.position,i):e.anchor&&this.options.anchorScrolling==="enabled"?this.viewportScroller.scrollToAnchor(e.anchor):this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.scrollToPosition([0,0])})}scheduleScrollEvent(e,i){if(this.isHydrating)return;let r=Le(this.transitions.currentNavigation)?.extras.scroll;this.zone.runOutsideAngular(async()=>{await new Promise(o=>{setTimeout(o),typeof requestAnimationFrame<"u"&&requestAnimationFrame(o)}),this.zone.run(()=>{this.transitions.events.next(new pa(e,this.lastSource==="popstate"?this.store[this.restoredId]:null,i,r))})})}ngOnDestroy(){this.routerEventsSubscription?.unsubscribe(),this.scrollEventsSubscription?.unsubscribe()}static \u0275fac=function(i){dv()};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();function Uy(t,...n){return Ti([{provide:ds,multi:!0,useValue:t},[],{provide:Hi,useFactory:iI},{provide:Xl,multi:!0,useFactory:rI},n.map(e=>e.\u0275providers)])}function iI(){return d(mt).routerState.root}function Mc(t,n){return{\u0275kind:t,\u0275providers:n}}function rI(){let t=d(Y);return n=>{let e=t.get(pn);if(n!==e.components[0])return;let i=t.get(mt),r=t.get(oI);t.get(Hy)===1&&i.initialNavigation(),t.get(lI,null,{optional:!0})?.setUpPreloading(),t.get(nI,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var oI=new b("",{factory:()=>new I}),Hy=new b("",{factory:()=>1});function sI(){let t=[{provide:mf,useValue:!0},{provide:Hy,useValue:0},If(()=>{let n=d(Y);return n.get(Vv,Promise.resolve()).then(()=>new Promise(i=>{let r=n.get(mt),o=n.get(oI);Rm(r,()=>{i(!0)}),n.get(Tm).afterPreactivation=()=>(i(!0),o.closed?q(void 0):o),r.initialNavigation()}))})];return Mc(2,t)}function aI(){let t=[If(()=>{d(mt).setUpLocationChangeListener()}),{provide:Hy,useValue:2}];return Mc(3,t)}var lI=new b("");function cI(t){return Mc(0,[{provide:lI,useExisting:tI},{provide:kc,useExisting:t}])}function dI(){return Mc(8,[Oy,{provide:Sc,useExisting:Oy}])}function uI(t){Vn("NgRouterViewTransitions");let n=[{provide:Ly,useValue:XS},{provide:By,useValue:w({skipNextTransition:!!t?.skipInitialTransition},t)}];return Mc(9,n)}var fI=[Bi,{provide:co,useClass:br},mt,ls,{provide:Hi,useFactory:iI},km,[]],$y=(()=>{class t{constructor(){}static forRoot(e,i){return{ngModule:t,providers:[fI,[],{provide:ds,multi:!0,useValue:e},[],i?.errorHandler?{provide:Vy,useValue:i.errorHandler}:[],{provide:uo,useValue:i||{}},i?.useHash?rB():oB(),iB(),i?.preloadingStrategy?cI(i.preloadingStrategy).\u0275providers:[],i?.initialNavigation?sB(i):[],i?.bindToComponentInputs?dI().\u0275providers:[],i?.enableViewTransitions?uI().\u0275providers:[],aB()]}}static forChild(e){return{ngModule:t,providers:[{provide:ds,multi:!0,useValue:e}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();function iB(){return{provide:nI,useFactory:()=>{let t=d(Qv),n=d(uo);return n.scrollOffset&&t.setOffset(n.scrollOffset),new nB(n)}}}function rB(){return{provide:di,useClass:Uv}}function oB(){return{provide:di,useClass:Hf}}function sB(t){return[t.initialNavigation==="disabled"?aI().\u0275providers:[],t.initialNavigation==="enabledBlocking"?sI().\u0275providers:[]]}var jy=new b("");function aB(){return[{provide:jy,useFactory:rI},{provide:Xl,multi:!0,useExisting:jy}]}var ke=(function(t){return t[t.State=0]="State",t[t.Transition=1]="Transition",t[t.Sequence=2]="Sequence",t[t.Group=3]="Group",t[t.Animate=4]="Animate",t[t.Keyframes=5]="Keyframes",t[t.Style=6]="Style",t[t.Trigger=7]="Trigger",t[t.Reference=8]="Reference",t[t.AnimateChild=9]="AnimateChild",t[t.AnimateRef=10]="AnimateRef",t[t.Query=11]="Query",t[t.Stagger=12]="Stagger",t})(ke||{}),mi="*";function mI(t,n=null){return{type:ke.Sequence,steps:t,options:n}}function Gy(t){return{type:ke.Style,styles:t,offset:null}}var wr=class{_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_originalOnDoneFns=[];_originalOnStartFns=[];_started=!1;_destroyed=!1;_finished=!1;_position=0;parentPlayer=null;totalTime;constructor(n=0,e=0){this.totalTime=n+e}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(n=>n()),this._onDoneFns=[])}onStart(n){this._originalOnStartFns.push(n),this._onStartFns.push(n)}onDone(n){this._originalOnDoneFns.push(n),this._onDoneFns.push(n)}onDestroy(n){this._onDestroyFns.push(n)}hasStarted(){return this._started}init(){}play(){this.hasStarted()||(this._onStart(),this.triggerMicrotask()),this._started=!0}triggerMicrotask(){queueMicrotask(()=>this._onFinish())}_onStart(){this._onStartFns.forEach(n=>n()),this._onStartFns=[]}pause(){}restart(){}finish(){this._onFinish()}destroy(){this._destroyed||(this._destroyed=!0,this.hasStarted()||this._onStart(),this.finish(),this._onDestroyFns.forEach(n=>n()),this._onDestroyFns=[])}reset(){this._started=!1,this._finished=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}setPosition(n){this._position=this.totalTime?n*this.totalTime:1}getPosition(){return this.totalTime?this._position/this.totalTime:1}triggerCallback(n){let e=n=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Ca=class{_onDoneFns=[];_onStartFns=[];_finished=!1;_started=!1;_destroyed=!1;_onDestroyFns=[];parentPlayer=null;totalTime=0;players;constructor(n){this.players=n;let e=0,i=0,r=0,o=this.players.length;o==0?queueMicrotask(()=>this._onFinish()):this.players.forEach(s=>{s.onDone(()=>{++e==o&&this._onFinish()}),s.onDestroy(()=>{++i==o&&this._onDestroy()}),s.onStart(()=>{++r==o&&this._onStart()})}),this.totalTime=this.players.reduce((s,a)=>Math.max(s,a.totalTime),0)}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(n=>n()),this._onDoneFns=[])}init(){this.players.forEach(n=>n.init())}onStart(n){this._onStartFns.push(n)}_onStart(){this.hasStarted()||(this._started=!0,this._onStartFns.forEach(n=>n()),this._onStartFns=[])}onDone(n){this._onDoneFns.push(n)}onDestroy(n){this._onDestroyFns.push(n)}hasStarted(){return this._started}play(){this.parentPlayer||this.init(),this._onStart(),this.players.forEach(n=>n.play())}pause(){this.players.forEach(n=>n.pause())}restart(){this.players.forEach(n=>n.restart())}finish(){this._onFinish(),this.players.forEach(n=>n.finish())}destroy(){this._onDestroy()}_onDestroy(){this._destroyed||(this._destroyed=!0,this._onFinish(),this.players.forEach(n=>n.destroy()),this._onDestroyFns.forEach(n=>n()),this._onDestroyFns=[])}reset(){this.players.forEach(n=>n.reset()),this._destroyed=!1,this._finished=!1,this._started=!1}setPosition(n){let e=n*this.totalTime;this.players.forEach(i=>{let r=i.totalTime?Math.min(1,e/i.totalTime):1;i.setPosition(r)})}getPosition(){let n=this.players.reduce((e,i)=>e===null||i.totalTime>e.totalTime?i:e,null);return n!=null?n.getPosition():0}beforeDestroy(){this.players.forEach(n=>{n.beforeDestroy&&n.beforeDestroy()})}triggerCallback(n){let e=n=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Tc="!";function hI(t){return new x(3e3,!1)}function lB(){return new x(3100,!1)}function cB(){return new x(3101,!1)}function dB(t){return new x(3001,!1)}function uB(t){return new x(3003,!1)}function fB(t){return new x(3004,!1)}function gI(t,n){return new x(3005,!1)}function _I(){return new x(3006,!1)}function vI(){return new x(3007,!1)}function yI(t,n){return new x(3008,!1)}function bI(t){return new x(3002,!1)}function wI(t,n,e,i,r){return new x(3010,!1)}function CI(){return new x(3011,!1)}function DI(){return new x(3012,!1)}function xI(){return new x(3200,!1)}function EI(){return new x(3202,!1)}function SI(){return new x(3013,!1)}function II(t){return new x(3014,!1)}function kI(t){return new x(3015,!1)}function MI(t){return new x(3016,!1)}function TI(t,n){return new x(3404,!1)}function mB(t){return new x(3502,!1)}function AI(t){return new x(3503,!1)}function RI(){return new x(3300,!1)}function OI(t){return new x(3504,!1)}function NI(t){return new x(3301,!1)}function FI(t,n){return new x(3302,!1)}function PI(t){return new x(3303,!1)}function LI(t,n){return new x(3400,!1)}function BI(t){return new x(3401,!1)}function VI(t){return new x(3402,!1)}function jI(t,n){return new x(3505,!1)}function Cr(t){switch(t.length){case 0:return new wr;case 1:return t[0];default:return new Ca(t)}}function Yy(t,n,e=new Map,i=new Map){let r=[],o=[],s=-1,a=null;if(n.forEach(l=>{let c=l.get("offset"),u=c==s,f=u&&a||new Map;l.forEach((h,m)=>{let _=m,D=h;if(m!=="offset")switch(_=t.normalizePropertyName(_,r),D){case Tc:D=e.get(m);break;case mi:D=i.get(m);break;default:D=t.normalizeStyleValue(m,_,D,r);break}f.set(_,D)}),u||o.push(f),a=f,s=c}),r.length)throw mB(r);return o}function Nm(t,n,e,i){switch(n){case"start":t.onStart(()=>i(e&&Wy(e,"start",t)));break;case"done":t.onDone(()=>i(e&&Wy(e,"done",t)));break;case"destroy":t.onDestroy(()=>i(e&&Wy(e,"destroy",t)));break}}function Wy(t,n,e){let i=e.totalTime,r=!!e.disabled,o=Fm(t.element,t.triggerName,t.fromState,t.toState,n||t.phaseName,i??t.totalTime,r),s=t._data;return s!=null&&(o._data=s),o}function Fm(t,n,e,i,r="",o=0,s){return{element:t,triggerName:n,fromState:e,toState:i,phaseName:r,totalTime:o,disabled:!!s}}function vn(t,n,e){let i=t.get(n);return i||t.set(n,i=e),i}function Ky(t){let n=t.indexOf(":"),e=t.substring(1,n),i=t.slice(n+1);return[e,i]}var hB=typeof document>"u"?null:document.documentElement;function Pm(t){let n=t.parentNode||t.host||null;return n===hB?null:n}function pB(t){return t.substring(1,6)=="ebkit"}var us=null,pI=!1;function zI(t){us||(us=gB()||{},pI=us.style?"WebkitAppearance"in us.style:!1);let n=!0;return us.style&&!pB(t)&&(n=t in us.style,!n&&pI&&(n="Webkit"+t.charAt(0).toUpperCase()+t.slice(1)in us.style)),n}function gB(){return typeof document<"u"?document.body:null}function Zy(t,n){for(;n;){if(n===t)return!0;n=Pm(n)}return!1}function Xy(t,n,e){if(e)return Array.from(t.querySelectorAll(n));let i=t.querySelector(n);return i?[i]:[]}var _B=1e3,Jy="{{",vB="}}",eb="ng-enter",Lm="ng-leave",Ac="ng-trigger",Rc=".ng-trigger",tb="ng-animating",Bm=".ng-animating";function $i(t){if(typeof t=="number")return t;let n=t.match(/^(-?[\.\d]+)(m?s)/);return!n||n.length<2?0:qy(parseFloat(n[1]),n[2])}function qy(t,n){return n==="s"?t*_B:t}function Oc(t,n,e){return t.hasOwnProperty("duration")?t:bB(t,n,e)}var yB=/^(-?[\.\d]+)(m?s)(?:\s+(-?[\.\d]+)(m?s))?(?:\s+([-a-z]+(?:\(.+?\))?))?$/i;function bB(t,n,e){let i,r=0,o="";if(typeof t=="string"){let s=t.match(yB);if(s===null)return n.push(hI(t)),{duration:0,delay:0,easing:""};i=qy(parseFloat(s[1]),s[2]);let a=s[3];a!=null&&(r=qy(parseFloat(a),s[4]));let l=s[5];l&&(o=l)}else i=t;if(!e){let s=!1,a=n.length;i<0&&(n.push(lB()),s=!0),r<0&&(n.push(cB()),s=!0),s&&n.splice(a,0,hI(t))}return{duration:i,delay:r,easing:o}}function UI(t){return t.length?t[0]instanceof Map?t:t.map(n=>new Map(Object.entries(n))):[]}function hi(t,n,e){n.forEach((i,r)=>{let o=Vm(r);e&&!e.has(r)&&e.set(r,t.style[o]),t.style[o]=i})}function fo(t,n){n.forEach((e,i)=>{let r=Vm(i);t.style[r]=""})}function Da(t){return Array.isArray(t)?t.length==1?t[0]:mI(t):t}function HI(t,n,e){let i=n.params||{},r=nb(t);r.length&&r.forEach(o=>{i.hasOwnProperty(o)||e.push(dB(o))})}var Qy=new RegExp(`${Jy}\\s*(.+?)\\s*${vB}`,"g");function nb(t){let n=[];if(typeof t=="string"){let e;for(;e=Qy.exec(t);)n.push(e[1]);Qy.lastIndex=0}return n}function xa(t,n,e){let i=`${t}`,r=i.replace(Qy,(o,s)=>{let a=n[s];return a==null&&(e.push(uB(s)),a=""),a.toString()});return r==i?t:r}var wB=/-+([a-z0-9])/g;function Vm(t){return t.replace(wB,(...n)=>n[1].toUpperCase())}function $I(t,n){return t===0||n===0}function GI(t,n,e){if(e.size&&n.length){let i=n[0],r=[];if(e.forEach((o,s)=>{i.has(s)||r.push(s),i.set(s,o)}),r.length)for(let o=1;o<n.length;o++){let s=n[o];r.forEach(a=>s.set(a,jm(t,a)))}}return n}function yn(t,n,e){switch(n.type){case ke.Trigger:return t.visitTrigger(n,e);case ke.State:return t.visitState(n,e);case ke.Transition:return t.visitTransition(n,e);case ke.Sequence:return t.visitSequence(n,e);case ke.Group:return t.visitGroup(n,e);case ke.Animate:return t.visitAnimate(n,e);case ke.Keyframes:return t.visitKeyframes(n,e);case ke.Style:return t.visitStyle(n,e);case ke.Reference:return t.visitReference(n,e);case ke.AnimateChild:return t.visitAnimateChild(n,e);case ke.AnimateRef:return t.visitAnimateRef(n,e);case ke.Query:return t.visitQuery(n,e);case ke.Stagger:return t.visitStagger(n,e);default:throw fB(n.type)}}function jm(t,n){return window.getComputedStyle(t)[n]}var yb=(()=>{class t{validateStyleProperty(e){return zI(e)}containsElement(e,i){return Zy(e,i)}getParentElement(e){return Pm(e)}query(e,i,r){return Xy(e,i,r)}computeStyle(e,i,r){return r||""}animate(e,i,r,o,s,a=[],l){return new wr(r,o)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),ms=class{static NOOP=new yb},hs=class{};var CB=new Set(["width","height","minWidth","minHeight","maxWidth","maxHeight","left","top","bottom","right","fontSize","outlineWidth","outlineOffset","paddingTop","paddingLeft","paddingBottom","paddingRight","marginTop","marginLeft","marginBottom","marginRight","borderRadius","borderWidth","borderTopWidth","borderLeftWidth","borderRightWidth","borderBottomWidth","textIndent","perspective"]),Gm=class extends hs{normalizePropertyName(n,e){return Vm(n)}normalizeStyleValue(n,e,i,r){let o="",s=i.toString().trim();if(CB.has(e)&&i!==0&&i!=="0")if(typeof i=="number")o="px";else{let a=i.match(/^[+-]?[\d\.]+([a-z]*)$/);a&&a[1].length==0&&r.push(gI(n,i))}return s+o}};var Wm="*";function DB(t,n){let e=[];return typeof t=="string"?t.split(/\s*,\s*/).forEach(i=>xB(i,e,n)):e.push(t),e}function xB(t,n,e){if(t[0]==":"){let l=EB(t,e);if(typeof l=="function"){n.push(l);return}t=l}let i=t.match(/^(\*|[-\w]+)\s*(<?[=-]>)\s*(\*|[-\w]+)$/);if(i==null||i.length<4)return e.push(kI(t)),n;let r=i[1],o=i[2],s=i[3];n.push(WI(r,s));let a=r==Wm&&s==Wm;o[0]=="<"&&!a&&n.push(WI(s,r))}function EB(t,n){switch(t){case":enter":return"void => *";case":leave":return"* => void";case":increment":return(e,i)=>parseFloat(i)>parseFloat(e);case":decrement":return(e,i)=>parseFloat(i)<parseFloat(e);default:return n.push(MI(t)),"* => *"}}var zm=new Set(["true","1"]),Um=new Set(["false","0"]);function WI(t,n){let e=zm.has(t)||Um.has(t),i=zm.has(n)||Um.has(n);return(r,o)=>{let s=t==Wm||t==r,a=n==Wm||n==o;return!s&&e&&typeof r=="boolean"&&(s=r?zm.has(t):Um.has(t)),!a&&i&&typeof o=="boolean"&&(a=o?zm.has(n):Um.has(n)),s&&a}}var nk=":self",SB=new RegExp(`s*${nk}s*,?`,"g");function ik(t,n,e,i){return new lb(t).build(n,e,i)}var qI="",lb=class{_driver;constructor(n){this._driver=n}build(n,e,i){let r=new cb(e);return this._resetContextStyleTimingState(r),yn(this,Da(n),r)}_resetContextStyleTimingState(n){n.currentQuerySelector=qI,n.collectedStyles=new Map,n.collectedStyles.set(qI,new Map),n.currentTime=0}visitTrigger(n,e){let i=e.queryCount=0,r=e.depCount=0,o=[],s=[];return n.name.charAt(0)=="@"&&e.errors.push(_I()),n.definitions.forEach(a=>{if(this._resetContextStyleTimingState(e),a.type==ke.State){let l=a,c=l.name;c.toString().split(/\s*,\s*/).forEach(u=>{l.name=u,o.push(this.visitState(l,e))}),l.name=c}else if(a.type==ke.Transition){let l=this.visitTransition(a,e);i+=l.queryCount,r+=l.depCount,s.push(l)}else e.errors.push(vI())}),{type:ke.Trigger,name:n.name,states:o,transitions:s,queryCount:i,depCount:r,options:null}}visitState(n,e){let i=this.visitStyle(n.styles,e),r=n.options&&n.options.params||null;if(i.containsDynamicStyles){let o=new Set,s=r||{};i.styles.forEach(a=>{a instanceof Map&&a.forEach(l=>{nb(l).forEach(c=>{s.hasOwnProperty(c)||o.add(c)})})}),o.size&&e.errors.push(yI(n.name,[...o.values()]))}return{type:ke.State,name:n.name,style:i,options:r?{params:r}:null}}visitTransition(n,e){e.queryCount=0,e.depCount=0;let i=yn(this,Da(n.animation),e),r=DB(n.expr,e.errors);return{type:ke.Transition,matchers:r,animation:i,queryCount:e.queryCount,depCount:e.depCount,options:fs(n.options)}}visitSequence(n,e){return{type:ke.Sequence,steps:n.steps.map(i=>yn(this,i,e)),options:fs(n.options)}}visitGroup(n,e){let i=e.currentTime,r=0,o=n.steps.map(s=>{e.currentTime=i;let a=yn(this,s,e);return r=Math.max(r,e.currentTime),a});return e.currentTime=r,{type:ke.Group,steps:o,options:fs(n.options)}}visitAnimate(n,e){let i=TB(n.timings,e.errors);e.currentAnimateTimings=i;let r,o=n.styles?n.styles:Gy({});if(o.type==ke.Keyframes)r=this.visitKeyframes(o,e);else{let s=n.styles,a=!1;if(!s){a=!0;let c={};i.easing&&(c.easing=i.easing),s=Gy(c)}e.currentTime+=i.duration+i.delay;let l=this.visitStyle(s,e);l.isEmptyStep=a,r=l}return e.currentAnimateTimings=null,{type:ke.Animate,timings:i,style:r,options:null}}visitStyle(n,e){let i=this._makeStyleAst(n,e);return this._validateStyleAst(i,e),i}_makeStyleAst(n,e){let i=[],r=Array.isArray(n.styles)?n.styles:[n.styles];for(let a of r)typeof a=="string"?a===mi?i.push(a):e.errors.push(bI(a)):i.push(new Map(Object.entries(a)));let o=!1,s=null;return i.forEach(a=>{if(a instanceof Map&&(a.has("easing")&&(s=a.get("easing"),a.delete("easing")),!o)){for(let l of a.values())if(l.toString().indexOf(Jy)>=0){o=!0;break}}}),{type:ke.Style,styles:i,easing:s,offset:n.offset,containsDynamicStyles:o,options:null}}_validateStyleAst(n,e){let i=e.currentAnimateTimings,r=e.currentTime,o=e.currentTime;i&&o>0&&(o-=i.duration+i.delay),n.styles.forEach(s=>{typeof s!="string"&&s.forEach((a,l)=>{let c=e.collectedStyles.get(e.currentQuerySelector),u=c.get(l),f=!0;u&&(o!=r&&o>=u.startTime&&r<=u.endTime&&(e.errors.push(wI(l,u.startTime,u.endTime,o,r)),f=!1),o=u.startTime),f&&c.set(l,{startTime:o,endTime:r}),e.options&&HI(a,e.options,e.errors)})})}visitKeyframes(n,e){let i={type:ke.Keyframes,styles:[],options:null};if(!e.currentAnimateTimings)return e.errors.push(CI()),i;let r=1,o=0,s=[],a=!1,l=!1,c=0,u=n.steps.map(M=>{let Z=this._makeStyleAst(M,e),Te=Z.offset!=null?Z.offset:MB(Z.styles),Ee=0;return Te!=null&&(o++,Ee=Z.offset=Te),l=l||Ee<0||Ee>1,a=a||Ee<c,c=Ee,s.push(Ee),Z});l&&e.errors.push(DI()),a&&e.errors.push(xI());let f=n.steps.length,h=0;o>0&&o<f?e.errors.push(EI()):o==0&&(h=r/(f-1));let m=f-1,_=e.currentTime,D=e.currentAnimateTimings,E=D.duration;return u.forEach((M,Z)=>{let Te=h>0?Z==m?1:h*Z:s[Z],Ee=Te*E;e.currentTime=_+D.delay+Ee,D.duration=Ee,this._validateStyleAst(M,e),M.offset=Te,i.styles.push(M)}),i}visitReference(n,e){return{type:ke.Reference,animation:yn(this,Da(n.animation),e),options:fs(n.options)}}visitAnimateChild(n,e){return e.depCount++,{type:ke.AnimateChild,options:fs(n.options)}}visitAnimateRef(n,e){return{type:ke.AnimateRef,animation:this.visitReference(n.animation,e),options:fs(n.options)}}visitQuery(n,e){let i=e.currentQuerySelector,r=n.options||{};e.queryCount++,e.currentQuery=n;let[o,s]=IB(n.selector);e.currentQuerySelector=i.length?i+" "+o:o,vn(e.collectedStyles,e.currentQuerySelector,new Map);let a=yn(this,Da(n.animation),e);return e.currentQuery=null,e.currentQuerySelector=i,{type:ke.Query,selector:o,limit:r.limit||0,optional:!!r.optional,includeSelf:s,animation:a,originalSelector:n.selector,options:fs(n.options)}}visitStagger(n,e){e.currentQuery||e.errors.push(SI());let i=n.timings==="full"?{duration:0,delay:0,easing:"full"}:Oc(n.timings,e.errors,!0);return{type:ke.Stagger,animation:yn(this,Da(n.animation),e),timings:i,options:null}}};function IB(t){let n=!!t.split(/\s*,\s*/).find(e=>e==nk);return n&&(t=t.replace(SB,"")),t=t.replace(/@\*/g,Rc).replace(/@\w+/g,e=>Rc+"-"+e.slice(1)).replace(/:animating/g,Bm),[t,n]}function kB(t){return t?w({},t):null}var cb=class{errors;queryCount=0;depCount=0;currentTransition=null;currentQuery=null;currentQuerySelector=null;currentAnimateTimings=null;currentTime=0;collectedStyles=new Map;options=null;unsupportedCSSPropertiesFound=new Set;constructor(n){this.errors=n}};function MB(t){if(typeof t=="string")return null;let n=null;if(Array.isArray(t))t.forEach(e=>{if(e instanceof Map&&e.has("offset")){let i=e;n=parseFloat(i.get("offset")),i.delete("offset")}});else if(t instanceof Map&&t.has("offset")){let e=t;n=parseFloat(e.get("offset")),e.delete("offset")}return n}function TB(t,n){if(t.hasOwnProperty("duration"))return t;if(typeof t=="number"){let o=Oc(t,n).duration;return ib(o,0,"")}let e=t;if(e.split(/\s+/).some(o=>o.charAt(0)=="{"&&o.charAt(1)=="{")){let o=ib(0,0,"");return o.dynamic=!0,o.strValue=e,o}let r=Oc(e,n);return ib(r.duration,r.delay,r.easing)}function fs(t){return t?(t=w({},t),t.params&&(t.params=kB(t.params))):t={},t}function ib(t,n,e){return{duration:t,delay:n,easing:e}}function bb(t,n,e,i,r,o,s=null,a=!1){return{type:1,element:t,keyframes:n,preStyleProps:e,postStyleProps:i,duration:r,delay:o,totalTime:r+o,easing:s,subTimeline:a}}var Fc=class{_map=new Map;get(n){return this._map.get(n)||[]}append(n,e){let i=this._map.get(n);i||this._map.set(n,i=[]),i.push(...e)}has(n){return this._map.has(n)}clear(){this._map.clear()}},AB=1,RB=":enter",OB=new RegExp(RB,"g"),NB=":leave",FB=new RegExp(NB,"g");function rk(t,n,e,i,r,o=new Map,s=new Map,a,l,c=[]){return new db().buildKeyframes(t,n,e,i,r,o,s,a,l,c)}var db=class{buildKeyframes(n,e,i,r,o,s,a,l,c,u=[]){c=c||new Fc;let f=new ub(n,e,c,r,o,u,[]);f.options=l;let h=l.delay?$i(l.delay):0;f.currentTimeline.delayNextStep(h),f.currentTimeline.setStyles([s],null,f.errors,l),yn(this,i,f);let m=f.timelines.filter(_=>_.containsAnimation());if(m.length&&a.size){let _;for(let D=m.length-1;D>=0;D--){let E=m[D];if(E.element===e){_=E;break}}_&&!_.allowOnlyTimelineStyles()&&_.setStyles([a],null,f.errors,l)}return m.length?m.map(_=>_.buildKeyframes()):[bb(e,[],[],[],0,h,"",!1)]}visitTrigger(n,e){}visitState(n,e){}visitTransition(n,e){}visitAnimateChild(n,e){let i=e.subInstructions.get(e.element);if(i){let r=e.createSubContext(n.options),o=e.currentTimeline.currentTime,s=this._visitSubInstructions(i,r,r.options);o!=s&&e.transformIntoNewTimeline(s)}e.previousNode=n}visitAnimateRef(n,e){let i=e.createSubContext(n.options);i.transformIntoNewTimeline(),this._applyAnimationRefDelays([n.options,n.animation.options],e,i),this.visitReference(n.animation,i),e.transformIntoNewTimeline(i.currentTimeline.currentTime),e.previousNode=n}_applyAnimationRefDelays(n,e,i){for(let r of n){let o=r?.delay;if(o){let s=typeof o=="number"?o:$i(xa(o,r?.params??{},e.errors));i.delayNextStep(s)}}}_visitSubInstructions(n,e,i){let o=e.currentTimeline.currentTime,s=i.duration!=null?$i(i.duration):null,a=i.delay!=null?$i(i.delay):null;return s!==0&&n.forEach(l=>{let c=e.appendInstructionToTimeline(l,s,a);o=Math.max(o,c.duration+c.delay)}),o}visitReference(n,e){e.updateOptions(n.options,!0),yn(this,n.animation,e),e.previousNode=n}visitSequence(n,e){let i=e.subContextCount,r=e,o=n.options;if(o&&(o.params||o.delay)&&(r=e.createSubContext(o),r.transformIntoNewTimeline(),o.delay!=null)){r.previousNode.type==ke.Style&&(r.currentTimeline.snapshotCurrentStyles(),r.previousNode=qm);let s=$i(o.delay);r.delayNextStep(s)}n.steps.length&&(n.steps.forEach(s=>yn(this,s,r)),r.currentTimeline.applyStylesToKeyframe(),r.subContextCount>i&&r.transformIntoNewTimeline()),e.previousNode=n}visitGroup(n,e){let i=[],r=e.currentTimeline.currentTime,o=n.options&&n.options.delay?$i(n.options.delay):0;n.steps.forEach(s=>{let a=e.createSubContext(n.options);o&&a.delayNextStep(o),yn(this,s,a),r=Math.max(r,a.currentTimeline.currentTime),i.push(a.currentTimeline)}),i.forEach(s=>e.currentTimeline.mergeTimelineCollectedStyles(s)),e.transformIntoNewTimeline(r),e.previousNode=n}_visitTiming(n,e){if(n.dynamic){let i=n.strValue,r=e.params?xa(i,e.params,e.errors):i;return Oc(r,e.errors)}else return{duration:n.duration,delay:n.delay,easing:n.easing}}visitAnimate(n,e){let i=e.currentAnimateTimings=this._visitTiming(n.timings,e),r=e.currentTimeline;i.delay&&(e.incrementTime(i.delay),r.snapshotCurrentStyles());let o=n.style;o.type==ke.Keyframes?this.visitKeyframes(o,e):(e.incrementTime(i.duration),this.visitStyle(o,e),r.applyStylesToKeyframe()),e.currentAnimateTimings=null,e.previousNode=n}visitStyle(n,e){let i=e.currentTimeline,r=e.currentAnimateTimings;!r&&i.hasCurrentStyleProperties()&&i.forwardFrame();let o=r&&r.easing||n.easing;n.isEmptyStep?i.applyEmptyStep(o):i.setStyles(n.styles,o,e.errors,e.options),e.previousNode=n}visitKeyframes(n,e){let i=e.currentAnimateTimings,r=e.currentTimeline.duration,o=i.duration,a=e.createSubContext().currentTimeline;a.easing=i.easing,n.styles.forEach(l=>{let c=l.offset||0;a.forwardTime(c*o),a.setStyles(l.styles,l.easing,e.errors,e.options),a.applyStylesToKeyframe()}),e.currentTimeline.mergeTimelineCollectedStyles(a),e.transformIntoNewTimeline(r+o),e.previousNode=n}visitQuery(n,e){let i=e.currentTimeline.currentTime,r=n.options||{},o=r.delay?$i(r.delay):0;o&&(e.previousNode.type===ke.Style||i==0&&e.currentTimeline.hasCurrentStyleProperties())&&(e.currentTimeline.snapshotCurrentStyles(),e.previousNode=qm);let s=i,a=e.invokeQuery(n.selector,n.originalSelector,n.limit,n.includeSelf,!!r.optional,e.errors);e.currentQueryTotal=a.length;let l=null;a.forEach((c,u)=>{e.currentQueryIndex=u;let f=e.createSubContext(n.options,c);o&&f.delayNextStep(o),c===e.element&&(l=f.currentTimeline),yn(this,n.animation,f),f.currentTimeline.applyStylesToKeyframe();let h=f.currentTimeline.currentTime;s=Math.max(s,h)}),e.currentQueryIndex=0,e.currentQueryTotal=0,e.transformIntoNewTimeline(s),l&&(e.currentTimeline.mergeTimelineCollectedStyles(l),e.currentTimeline.snapshotCurrentStyles()),e.previousNode=n}visitStagger(n,e){let i=e.parentContext,r=e.currentTimeline,o=n.timings,s=Math.abs(o.duration),a=s*(e.currentQueryTotal-1),l=s*e.currentQueryIndex;switch(o.duration<0?"reverse":o.easing){case"reverse":l=a-l;break;case"full":l=i.currentStaggerTime;break}let u=e.currentTimeline;l&&u.delayNextStep(l);let f=u.currentTime;yn(this,n.animation,e),e.previousNode=n,i.currentStaggerTime=r.currentTime-f+(r.startTime-i.currentTimeline.startTime)}},qm={},ub=class t{_driver;element;subInstructions;_enterClassName;_leaveClassName;errors;timelines;parentContext=null;currentTimeline;currentAnimateTimings=null;previousNode=qm;subContextCount=0;options={};currentQueryIndex=0;currentQueryTotal=0;currentStaggerTime=0;constructor(n,e,i,r,o,s,a,l){this._driver=n,this.element=e,this.subInstructions=i,this._enterClassName=r,this._leaveClassName=o,this.errors=s,this.timelines=a,this.currentTimeline=l||new Qm(this._driver,e,0),a.push(this.currentTimeline)}get params(){return this.options.params}updateOptions(n,e){if(!n)return;let i=n,r=this.options;i.duration!=null&&(r.duration=$i(i.duration)),i.delay!=null&&(r.delay=$i(i.delay));let o=i.params;if(o){let s=r.params;s||(s=this.options.params={}),Object.keys(o).forEach(a=>{(!e||!s.hasOwnProperty(a))&&(s[a]=xa(o[a],s,this.errors))})}}_copyOptions(){let n={};if(this.options){let e=this.options.params;if(e){let i=n.params={};Object.keys(e).forEach(r=>{i[r]=e[r]})}}return n}createSubContext(n=null,e,i){let r=e||this.element,o=new t(this._driver,r,this.subInstructions,this._enterClassName,this._leaveClassName,this.errors,this.timelines,this.currentTimeline.fork(r,i||0));return o.previousNode=this.previousNode,o.currentAnimateTimings=this.currentAnimateTimings,o.options=this._copyOptions(),o.updateOptions(n),o.currentQueryIndex=this.currentQueryIndex,o.currentQueryTotal=this.currentQueryTotal,o.parentContext=this,this.subContextCount++,o}transformIntoNewTimeline(n){return this.previousNode=qm,this.currentTimeline=this.currentTimeline.fork(this.element,n),this.timelines.push(this.currentTimeline),this.currentTimeline}appendInstructionToTimeline(n,e,i){let r={duration:e??n.duration,delay:this.currentTimeline.currentTime+(i??0)+n.delay,easing:""},o=new fb(this._driver,n.element,n.keyframes,n.preStyleProps,n.postStyleProps,r,n.stretchStartingKeyframe);return this.timelines.push(o),r}incrementTime(n){this.currentTimeline.forwardTime(this.currentTimeline.duration+n)}delayNextStep(n){n>0&&this.currentTimeline.delayNextStep(n)}invokeQuery(n,e,i,r,o,s){let a=[];if(r&&a.push(this.element),n.length>0){n=n.replace(OB,"."+this._enterClassName),n=n.replace(FB,"."+this._leaveClassName);let l=i!=1,c=this._driver.query(this.element,n,l);i!==0&&(c=i<0?c.slice(c.length+i,c.length):c.slice(0,i)),a.push(...c)}return!o&&a.length==0&&s.push(II(e)),a}},Qm=class t{_driver;element;startTime;_elementTimelineStylesLookup;duration=0;easing=null;_previousKeyframe=new Map;_currentKeyframe=new Map;_keyframes=new Map;_styleSummary=new Map;_localTimelineStyles=new Map;_globalTimelineStyles;_pendingStyles=new Map;_backFill=new Map;_currentEmptyStepKeyframe=null;constructor(n,e,i,r){this._driver=n,this.element=e,this.startTime=i,this._elementTimelineStylesLookup=r,this._elementTimelineStylesLookup||(this._elementTimelineStylesLookup=new Map),this._globalTimelineStyles=this._elementTimelineStylesLookup.get(e),this._globalTimelineStyles||(this._globalTimelineStyles=this._localTimelineStyles,this._elementTimelineStylesLookup.set(e,this._localTimelineStyles)),this._loadKeyframe()}containsAnimation(){switch(this._keyframes.size){case 0:return!1;case 1:return this.hasCurrentStyleProperties();default:return!0}}hasCurrentStyleProperties(){return this._currentKeyframe.size>0}get currentTime(){return this.startTime+this.duration}delayNextStep(n){let e=this._keyframes.size===1&&this._pendingStyles.size;this.duration||e?(this.forwardTime(this.currentTime+n),e&&this.snapshotCurrentStyles()):this.startTime+=n}fork(n,e){return this.applyStylesToKeyframe(),new t(this._driver,n,e||this.currentTime,this._elementTimelineStylesLookup)}_loadKeyframe(){this._currentKeyframe&&(this._previousKeyframe=this._currentKeyframe),this._currentKeyframe=this._keyframes.get(this.duration),this._currentKeyframe||(this._currentKeyframe=new Map,this._keyframes.set(this.duration,this._currentKeyframe))}forwardFrame(){this.duration+=AB,this._loadKeyframe()}forwardTime(n){this.applyStylesToKeyframe(),this.duration=n,this._loadKeyframe()}_updateStyle(n,e){this._localTimelineStyles.set(n,e),this._globalTimelineStyles.set(n,e),this._styleSummary.set(n,{time:this.currentTime,value:e})}allowOnlyTimelineStyles(){return this._currentEmptyStepKeyframe!==this._currentKeyframe}applyEmptyStep(n){n&&this._previousKeyframe.set("easing",n);for(let[e,i]of this._globalTimelineStyles)this._backFill.set(e,i||mi),this._currentKeyframe.set(e,mi);this._currentEmptyStepKeyframe=this._currentKeyframe}setStyles(n,e,i,r){e&&this._previousKeyframe.set("easing",e);let o=r&&r.params||{},s=PB(n,this._globalTimelineStyles);for(let[a,l]of s){let c=xa(l,o,i);this._pendingStyles.set(a,c),this._localTimelineStyles.has(a)||this._backFill.set(a,this._globalTimelineStyles.get(a)??mi),this._updateStyle(a,c)}}applyStylesToKeyframe(){this._pendingStyles.size!=0&&(this._pendingStyles.forEach((n,e)=>{this._currentKeyframe.set(e,n)}),this._pendingStyles.clear(),this._localTimelineStyles.forEach((n,e)=>{this._currentKeyframe.has(e)||this._currentKeyframe.set(e,n)}))}snapshotCurrentStyles(){for(let[n,e]of this._localTimelineStyles)this._pendingStyles.set(n,e),this._updateStyle(n,e)}getFinalKeyframe(){return this._keyframes.get(this.duration)}get properties(){let n=[];for(let e in this._currentKeyframe)n.push(e);return n}mergeTimelineCollectedStyles(n){n._styleSummary.forEach((e,i)=>{let r=this._styleSummary.get(i);(!r||e.time>r.time)&&this._updateStyle(i,e.value)})}buildKeyframes(){this.applyStylesToKeyframe();let n=new Set,e=new Set,i=this._keyframes.size===1&&this.duration===0,r=[];this._keyframes.forEach((a,l)=>{let c=new Map([...this._backFill,...a]);c.forEach((u,f)=>{u===Tc?n.add(f):u===mi&&e.add(f)}),i||c.set("offset",l/this.duration),r.push(c)});let o=[...n.values()],s=[...e.values()];if(i){let a=r[0],l=new Map(a);a.set("offset",0),l.set("offset",1),r=[a,l]}return bb(this.element,r,o,s,this.duration,this.startTime,this.easing,!1)}},fb=class extends Qm{keyframes;preStyleProps;postStyleProps;_stretchStartingKeyframe;timings;constructor(n,e,i,r,o,s,a=!1){super(n,e,s.delay),this.keyframes=i,this.preStyleProps=r,this.postStyleProps=o,this._stretchStartingKeyframe=a,this.timings={duration:s.duration,delay:s.delay,easing:s.easing}}containsAnimation(){return this.keyframes.length>1}buildKeyframes(){let n=this.keyframes,{delay:e,duration:i,easing:r}=this.timings;if(this._stretchStartingKeyframe&&e){let o=[],s=i+e,a=e/s,l=new Map(n[0]);l.set("offset",0),o.push(l);let c=new Map(n[0]);c.set("offset",QI(a)),o.push(c);let u=n.length-1;for(let f=1;f<=u;f++){let h=new Map(n[f]),m=h.get("offset"),_=e+m*i;h.set("offset",QI(_/s)),o.push(h)}i=s,e=0,r="",n=o}return bb(this.element,n,this.preStyleProps,this.postStyleProps,i,e,r,!0)}};function QI(t,n=3){let e=Math.pow(10,n-1);return Math.round(t*e)/e}function PB(t,n){let e=new Map,i;return t.forEach(r=>{if(r==="*"){i??=n.keys();for(let o of i)e.set(o,mi)}else for(let[o,s]of r)e.set(o,s)}),e}function YI(t,n,e,i,r,o,s,a,l,c,u,f,h){return{type:0,element:t,triggerName:n,isRemovalTransition:r,fromState:e,fromStyles:o,toState:i,toStyles:s,timelines:a,queriedElements:l,preStyleProps:c,postStyleProps:u,totalTime:f,errors:h}}var rb={},Ym=class{_triggerName;ast;_stateStyles;constructor(n,e,i){this._triggerName=n,this.ast=e,this._stateStyles=i}match(n,e,i,r){return LB(this.ast.matchers,n,e,i,r)}buildStyles(n,e,i){let r=this._stateStyles.get("*");return n!==void 0&&(r=this._stateStyles.get(n?.toString())||r),r?r.buildStyles(e,i):new Map}build(n,e,i,r,o,s,a,l,c,u){let f=[],h=this.ast.options&&this.ast.options.params||rb,m=a&&a.params||rb,_=this.buildStyles(i,m,f),D=l&&l.params||rb,E=this.buildStyles(r,D,f),M=new Set,Z=new Map,Te=new Map,Ee=r==="void",Xt={params:ok(D,h),delay:this.ast.options?.delay},pt=u?[]:rk(n,e,this.ast.animation,o,s,_,E,Xt,c,f),nt=0;return pt.forEach(fn=>{nt=Math.max(fn.duration+fn.delay,nt)}),f.length?YI(e,this._triggerName,i,r,Ee,_,E,[],[],Z,Te,nt,f):(pt.forEach(fn=>{let wo=fn.element,ks=vn(Z,wo,new Set);fn.preStyleProps.forEach(Co=>ks.add(Co));let $0=vn(Te,wo,new Set);fn.postStyleProps.forEach(Co=>$0.add(Co)),wo!==e&&M.add(wo)}),YI(e,this._triggerName,i,r,Ee,_,E,pt,[...M.values()],Z,Te,nt))}};function LB(t,n,e,i,r){return t.some(o=>o(n,e,i,r))}function ok(t,n){let e=w({},n);return Object.entries(t).forEach(([i,r])=>{r!=null&&(e[i]=r)}),e}var mb=class{styles;defaultParams;normalizer;constructor(n,e,i){this.styles=n,this.defaultParams=e,this.normalizer=i}buildStyles(n,e){let i=new Map,r=ok(n,this.defaultParams);return this.styles.styles.forEach(o=>{typeof o!="string"&&o.forEach((s,a)=>{s&&(s=xa(s,r,e));let l=this.normalizer.normalizePropertyName(a,e);s=this.normalizer.normalizeStyleValue(a,l,s,e),i.set(a,s)})}),i}};function BB(t,n,e){return new hb(t,n,e)}var hb=class{name;ast;_normalizer;transitionFactories=[];fallbackTransition;states=new Map;constructor(n,e,i){this.name=n,this.ast=e,this._normalizer=i,e.states.forEach(r=>{let o=r.options&&r.options.params||{};this.states.set(r.name,new mb(r.style,o,i))}),KI(this.states,"true","1"),KI(this.states,"false","0"),e.transitions.forEach(r=>{this.transitionFactories.push(new Ym(n,r,this.states))}),this.fallbackTransition=VB(n,this.states)}get containsQueries(){return this.ast.queryCount>0}matchTransition(n,e,i,r){return this.transitionFactories.find(s=>s.match(n,e,i,r))||null}matchStyles(n,e,i){return this.fallbackTransition.buildStyles(n,e,i)}};function VB(t,n,e){let i=[(s,a)=>!0],r={type:ke.Sequence,steps:[],options:null},o={type:ke.Transition,animation:r,matchers:i,options:null,queryCount:0,depCount:0};return new Ym(t,o,n)}function KI(t,n,e){t.has(n)?t.has(e)||t.set(e,t.get(n)):t.has(e)&&t.set(n,t.get(e))}var jB=new Fc,pb=class{bodyNode;_driver;_normalizer;_animations=new Map;_playersById=new Map;players=[];constructor(n,e,i){this.bodyNode=n,this._driver=e,this._normalizer=i}register(n,e){let i=[],r=[],o=ik(this._driver,e,i,r);if(i.length)throw AI(i);this._animations.set(n,o)}_buildPlayer(n,e,i){let r=n.element,o=Yy(this._normalizer,n.keyframes,e,i);return this._driver.animate(r,o,n.duration,n.delay,n.easing,[],!0)}create(n,e,i={}){let r=[],o=this._animations.get(n),s,a=new Map;if(o?(s=rk(this._driver,e,o,eb,Lm,new Map,new Map,i,jB,r),s.forEach(u=>{let f=vn(a,u.element,new Map);u.postStyleProps.forEach(h=>f.set(h,null))})):(r.push(RI()),s=[]),r.length)throw OI(r);a.forEach((u,f)=>{u.forEach((h,m)=>{u.set(m,this._driver.computeStyle(f,m,mi))})});let l=s.map(u=>{let f=a.get(u.element);return this._buildPlayer(u,new Map,f)}),c=Cr(l);return this._playersById.set(n,c),c.onDestroy(()=>this.destroy(n)),this.players.push(c),c}destroy(n){let e=this._getPlayer(n);e.destroy(),this._playersById.delete(n);let i=this.players.indexOf(e);i>=0&&this.players.splice(i,1)}_getPlayer(n){let e=this._playersById.get(n);if(!e)throw NI(n);return e}listen(n,e,i,r){let o=Fm(e,"","","");return Nm(this._getPlayer(n),i,o,r),()=>{}}command(n,e,i,r){if(i=="register"){this.register(n,r[0]);return}if(i=="create"){let s=r[0]||{};this.create(n,e,s);return}let o=this._getPlayer(n);switch(i){case"play":o.play();break;case"pause":o.pause();break;case"reset":o.reset();break;case"restart":o.restart();break;case"finish":o.finish();break;case"init":o.init();break;case"setPosition":o.setPosition(parseFloat(r[0]));break;case"destroy":this.destroy(n);break}}},ZI="ng-animate-queued",zB=".ng-animate-queued",ob="ng-animate-disabled",UB=".ng-animate-disabled",HB="ng-star-inserted",$B=".ng-star-inserted",GB=[],sk={namespaceId:"",setForRemoval:!1,setForMove:!1,hasAnimation:!1,removedBeforeQueried:!1},WB={namespaceId:"",setForMove:!1,setForRemoval:!1,hasAnimation:!1,removedBeforeQueried:!0},pi="__ng_removed",Pc=class{namespaceId;value;options;get params(){return this.options.params}constructor(n,e=""){this.namespaceId=e;let i=n&&n.hasOwnProperty("value"),r=i?n.value:n;if(this.value=QB(r),i){let o=n,{value:s}=o,a=mp(o,["value"]);this.options=a}else this.options={};this.options.params||(this.options.params={})}absorbOptions(n){let e=n.params;if(e){let i=this.options.params;Object.keys(e).forEach(r=>{i[r]==null&&(i[r]=e[r])})}}},Nc="void",sb=new Pc(Nc),gb=class{id;hostElement;_engine;players=[];_triggers=new Map;_queue=[];_elementListeners=new Map;_hostClassName;constructor(n,e,i){this.id=n,this.hostElement=e,this._engine=i,this._hostClassName="ng-tns-"+n,Hn(e,this._hostClassName)}listen(n,e,i,r){if(!this._triggers.has(e))throw FI(i,e);if(i==null||i.length==0)throw PI(e);if(!YB(i))throw LI(i,e);let o=vn(this._elementListeners,n,[]),s={name:e,phase:i,callback:r};o.push(s);let a=vn(this._engine.statesByElement,n,new Map);return a.has(e)||(Hn(n,Ac),Hn(n,Ac+"-"+e),a.set(e,sb)),()=>{this._engine.afterFlush(()=>{let l=o.indexOf(s);l>=0&&o.splice(l,1),this._triggers.has(e)||a.delete(e)})}}register(n,e){return this._triggers.has(n)?!1:(this._triggers.set(n,e),!0)}_getTrigger(n){let e=this._triggers.get(n);if(!e)throw BI(n);return e}trigger(n,e,i,r=!0){let o=this._getTrigger(e),s=new Lc(this.id,e,n),a=this._engine.statesByElement.get(n);a||(Hn(n,Ac),Hn(n,Ac+"-"+e),this._engine.statesByElement.set(n,a=new Map));let l=a.get(e),c=new Pc(i,this.id);if(!(i&&i.hasOwnProperty("value"))&&l&&c.absorbOptions(l.options),a.set(e,c),l||(l=sb),!(c.value===Nc)&&l.value===c.value){if(!XB(l.params,c.params)){let D=[],E=o.matchStyles(l.value,l.params,D),M=o.matchStyles(c.value,c.params,D);D.length?this._engine.reportError(D):this._engine.afterFlush(()=>{fo(n,E),hi(n,M)})}return}let h=vn(this._engine.playersByElement,n,[]);h.forEach(D=>{D.namespaceId==this.id&&D.triggerName==e&&D.queued&&D.destroy()});let m=o.matchTransition(l.value,c.value,n,c.params),_=!1;if(!m){if(!r)return;m=o.fallbackTransition,_=!0}return this._engine.totalQueuedPlayers++,this._queue.push({element:n,triggerName:e,transition:m,fromState:l,toState:c,player:s,isFallbackTransition:_}),_||(Hn(n,ZI),s.onStart(()=>{Ea(n,ZI)})),s.onDone(()=>{let D=this.players.indexOf(s);D>=0&&this.players.splice(D,1);let E=this._engine.playersByElement.get(n);if(E){let M=E.indexOf(s);M>=0&&E.splice(M,1)}}),this.players.push(s),h.push(s),s}deregister(n){this._triggers.delete(n),this._engine.statesByElement.forEach(e=>e.delete(n)),this._elementListeners.forEach((e,i)=>{this._elementListeners.set(i,e.filter(r=>r.name!=n))})}clearElementCache(n){this._engine.statesByElement.delete(n),this._elementListeners.delete(n);let e=this._engine.playersByElement.get(n);e&&(e.forEach(i=>i.destroy()),this._engine.playersByElement.delete(n))}_signalRemovalForInnerTriggers(n,e){let i=this._engine.driver.query(n,Rc,!0);i.forEach(r=>{if(r[pi])return;let o=this._engine.fetchNamespacesByElement(r);o.size?o.forEach(s=>s.triggerLeaveAnimation(r,e,!1,!0)):this.clearElementCache(r)}),this._engine.afterFlushAnimationsDone(()=>i.forEach(r=>this.clearElementCache(r)))}triggerLeaveAnimation(n,e,i,r){let o=this._engine.statesByElement.get(n),s=new Map;if(o){let a=[];if(o.forEach((l,c)=>{if(s.set(c,l.value),this._triggers.has(c)){let u=this.trigger(n,c,Nc,r);u&&a.push(u)}}),a.length)return this._engine.markElementAsRemoved(this.id,n,!0,e,s),i&&Cr(a).onDone(()=>this._engine.processLeaveNode(n)),!0}return!1}prepareLeaveAnimationListeners(n){let e=this._elementListeners.get(n),i=this._engine.statesByElement.get(n);if(e&&i){let r=new Set;e.forEach(o=>{let s=o.name;if(r.has(s))return;r.add(s);let l=this._triggers.get(s).fallbackTransition,c=i.get(s)||sb,u=new Pc(Nc),f=new Lc(this.id,s,n);this._engine.totalQueuedPlayers++,this._queue.push({element:n,triggerName:s,transition:l,fromState:c,toState:u,player:f,isFallbackTransition:!0})})}}removeNode(n,e){let i=this._engine;if(n.childElementCount&&this._signalRemovalForInnerTriggers(n,e),this.triggerLeaveAnimation(n,e,!0))return;let r=!1;if(i.totalAnimations){let o=i.players.length?i.playersByQueriedElement.get(n):[];if(o&&o.length)r=!0;else{let s=n;for(;s=s.parentNode;)if(i.statesByElement.get(s)){r=!0;break}}}if(this.prepareLeaveAnimationListeners(n),r)i.markElementAsRemoved(this.id,n,!1,e);else{let o=n[pi];(!o||o===sk)&&(i.afterFlush(()=>this.clearElementCache(n)),i.destroyInnerAnimations(n),i._onRemovalComplete(n,e))}}insertNode(n,e){Hn(n,this._hostClassName)}drainQueuedTransitions(n){let e=[];return this._queue.forEach(i=>{let r=i.player;if(r.destroyed)return;let o=i.element,s=this._elementListeners.get(o);s&&s.forEach(a=>{if(a.name==i.triggerName){let l=Fm(o,i.triggerName,i.fromState.value,i.toState.value);l._data=n,Nm(i.player,a.phase,l,a.callback)}}),r.markedForDestroy?this._engine.afterFlush(()=>{r.destroy()}):e.push(i)}),this._queue=[],e.sort((i,r)=>{let o=i.transition.ast.depCount,s=r.transition.ast.depCount;return o==0||s==0?o-s:this._engine.driver.containsElement(i.element,r.element)?1:-1})}destroy(n){this.players.forEach(e=>e.destroy()),this._signalRemovalForInnerTriggers(this.hostElement,n)}},_b=class{bodyNode;driver;_normalizer;players=[];newHostElements=new Map;playersByElement=new Map;playersByQueriedElement=new Map;statesByElement=new Map;disabledNodes=new Set;totalAnimations=0;totalQueuedPlayers=0;_namespaceLookup={};_namespaceList=[];_flushFns=[];_whenQuietFns=[];namespacesByHostElement=new Map;collectedEnterElements=[];collectedLeaveElements=[];onRemovalComplete=(n,e)=>{};_onRemovalComplete(n,e){this.onRemovalComplete(n,e)}constructor(n,e,i){this.bodyNode=n,this.driver=e,this._normalizer=i}get queuedPlayers(){let n=[];return this._namespaceList.forEach(e=>{e.players.forEach(i=>{i.queued&&n.push(i)})}),n}createNamespace(n,e){let i=new gb(n,e,this);return this.bodyNode&&this.driver.containsElement(this.bodyNode,e)?this._balanceNamespaceList(i,e):(this.newHostElements.set(e,i),this.collectEnterElement(e)),this._namespaceLookup[n]=i}_balanceNamespaceList(n,e){let i=this._namespaceList,r=this.namespacesByHostElement;if(i.length-1>=0){let s=!1,a=this.driver.getParentElement(e);for(;a;){let l=r.get(a);if(l){let c=i.indexOf(l);i.splice(c+1,0,n),s=!0;break}a=this.driver.getParentElement(a)}s||i.unshift(n)}else i.push(n);return r.set(e,n),n}register(n,e){let i=this._namespaceLookup[n];return i||(i=this.createNamespace(n,e)),i}registerTrigger(n,e,i){let r=this._namespaceLookup[n];r&&r.register(e,i)&&this.totalAnimations++}destroy(n,e){n&&(this.afterFlush(()=>{}),this.afterFlushAnimationsDone(()=>{let i=this._fetchNamespace(n);this.namespacesByHostElement.delete(i.hostElement);let r=this._namespaceList.indexOf(i);r>=0&&this._namespaceList.splice(r,1),i.destroy(e),delete this._namespaceLookup[n]}))}_fetchNamespace(n){return this._namespaceLookup[n]}fetchNamespacesByElement(n){let e=new Set,i=this.statesByElement.get(n);if(i){for(let r of i.values())if(r.namespaceId){let o=this._fetchNamespace(r.namespaceId);o&&e.add(o)}}return e}trigger(n,e,i,r){if(Hm(e)){let o=this._fetchNamespace(n);if(o)return o.trigger(e,i,r),!0}return!1}insertNode(n,e,i,r){if(!Hm(e))return;let o=e[pi];if(o&&o.setForRemoval){o.setForRemoval=!1,o.setForMove=!0;let s=this.collectedLeaveElements.indexOf(e);s>=0&&this.collectedLeaveElements.splice(s,1)}if(n){let s=this._fetchNamespace(n);s&&s.insertNode(e,i)}r&&this.collectEnterElement(e)}collectEnterElement(n){this.collectedEnterElements.push(n)}markElementAsDisabled(n,e){e?this.disabledNodes.has(n)||(this.disabledNodes.add(n),Hn(n,ob)):this.disabledNodes.has(n)&&(this.disabledNodes.delete(n),Ea(n,ob))}removeNode(n,e,i){if(Hm(e)){let r=n?this._fetchNamespace(n):null;r?r.removeNode(e,i):this.markElementAsRemoved(n,e,!1,i);let o=this.namespacesByHostElement.get(e);o&&o.id!==n&&o.removeNode(e,i)}else this._onRemovalComplete(e,i)}markElementAsRemoved(n,e,i,r,o){this.collectedLeaveElements.push(e),e[pi]={namespaceId:n,setForRemoval:r,hasAnimation:i,removedBeforeQueried:!1,previousTriggersValues:o}}listen(n,e,i,r,o){return Hm(e)?this._fetchNamespace(n).listen(e,i,r,o):()=>{}}_buildInstruction(n,e,i,r,o){return n.transition.build(this.driver,n.element,n.fromState.value,n.toState.value,i,r,n.fromState.options,n.toState.options,e,o)}destroyInnerAnimations(n){let e=this.driver.query(n,Rc,!0);e.forEach(i=>this.destroyActiveAnimationsForElement(i)),this.playersByQueriedElement.size!=0&&(e=this.driver.query(n,Bm,!0),e.forEach(i=>this.finishActiveQueriedAnimationOnElement(i)))}destroyActiveAnimationsForElement(n){let e=this.playersByElement.get(n);e&&e.forEach(i=>{i.queued?i.markedForDestroy=!0:i.destroy()})}finishActiveQueriedAnimationOnElement(n){let e=this.playersByQueriedElement.get(n);e&&e.forEach(i=>i.finish())}whenRenderingDone(){return new Promise(n=>{if(this.players.length)return Cr(this.players).onDone(()=>n());n()})}processLeaveNode(n){let e=n[pi];if(e&&e.setForRemoval){if(n[pi]=sk,e.namespaceId){this.destroyInnerAnimations(n);let i=this._fetchNamespace(e.namespaceId);i&&i.clearElementCache(n)}this._onRemovalComplete(n,e.setForRemoval)}n.classList?.contains(ob)&&this.markElementAsDisabled(n,!1),this.driver.query(n,UB,!0).forEach(i=>{this.markElementAsDisabled(i,!1)})}flush(n=-1){let e=[];if(this.newHostElements.size&&(this.newHostElements.forEach((i,r)=>this._balanceNamespaceList(i,r)),this.newHostElements.clear()),this.totalAnimations&&this.collectedEnterElements.length)for(let i=0;i<this.collectedEnterElements.length;i++){let r=this.collectedEnterElements[i];Hn(r,HB)}if(this._namespaceList.length&&(this.totalQueuedPlayers||this.collectedLeaveElements.length)){let i=[];try{e=this._flushAnimations(i,n)}finally{for(let r=0;r<i.length;r++)i[r]()}}else for(let i=0;i<this.collectedLeaveElements.length;i++){let r=this.collectedLeaveElements[i];this.processLeaveNode(r)}if(this.totalQueuedPlayers=0,this.collectedEnterElements.length=0,this.collectedLeaveElements.length=0,this._flushFns.forEach(i=>i()),this._flushFns=[],this._whenQuietFns.length){let i=this._whenQuietFns;this._whenQuietFns=[],e.length?Cr(e).onDone(()=>{i.forEach(r=>r())}):i.forEach(r=>r())}}reportError(n){throw VI(n)}_flushAnimations(n,e){let i=new Fc,r=[],o=new Map,s=[],a=new Map,l=new Map,c=new Map,u=new Set;this.disabledNodes.forEach(K=>{u.add(K);let ne=this.driver.query(K,zB,!0);for(let ae=0;ae<ne.length;ae++)u.add(ne[ae])});let f=this.bodyNode,h=Array.from(this.statesByElement.keys()),m=ek(h,this.collectedEnterElements),_=new Map,D=0;m.forEach((K,ne)=>{let ae=eb+D++;_.set(ne,ae),K.forEach(Ne=>Hn(Ne,ae))});let E=[],M=new Set,Z=new Set;for(let K=0;K<this.collectedLeaveElements.length;K++){let ne=this.collectedLeaveElements[K],ae=ne[pi];ae&&ae.setForRemoval&&(E.push(ne),M.add(ne),ae.hasAnimation?this.driver.query(ne,$B,!0).forEach(Ne=>M.add(Ne)):Z.add(ne))}let Te=new Map,Ee=ek(h,Array.from(M));Ee.forEach((K,ne)=>{let ae=Lm+D++;Te.set(ne,ae),K.forEach(Ne=>Hn(Ne,ae))}),n.push(()=>{m.forEach((K,ne)=>{let ae=_.get(ne);K.forEach(Ne=>Ea(Ne,ae))}),Ee.forEach((K,ne)=>{let ae=Te.get(ne);K.forEach(Ne=>Ea(Ne,ae))}),E.forEach(K=>{this.processLeaveNode(K)})});let Xt=[],pt=[];for(let K=this._namespaceList.length-1;K>=0;K--)this._namespaceList[K].drainQueuedTransitions(e).forEach(ae=>{let Ne=ae.player,xt=ae.element;if(Xt.push(Ne),this.collectedEnterElements.length){let Vt=xt[pi];if(Vt&&Vt.setForMove){if(Vt.previousTriggersValues&&Vt.previousTriggersValues.has(ae.triggerName)){let Do=Vt.previousTriggersValues.get(ae.triggerName),Tn=this.statesByElement.get(ae.element);if(Tn&&Tn.has(ae.triggerName)){let Ed=Tn.get(ae.triggerName);Ed.value=Do,Tn.set(ae.triggerName,Ed)}}Ne.destroy();return}}let Di=!f||!this.driver.containsElement(f,xt),Cn=Te.get(xt),Fr=_.get(xt),it=this._buildInstruction(ae,i,Fr,Cn,Di);if(it.errors&&it.errors.length){pt.push(it);return}if(Di){Ne.onStart(()=>fo(xt,it.fromStyles)),Ne.onDestroy(()=>hi(xt,it.toStyles)),r.push(Ne);return}if(ae.isFallbackTransition){Ne.onStart(()=>fo(xt,it.fromStyles)),Ne.onDestroy(()=>hi(xt,it.toStyles)),r.push(Ne);return}let q0=[];it.timelines.forEach(Vt=>{Vt.stretchStartingKeyframe=!0,this.disabledNodes.has(Vt.element)||q0.push(Vt)}),it.timelines=q0,i.append(xt,it.timelines);let vA={instruction:it,player:Ne,element:xt};s.push(vA),it.queriedElements.forEach(Vt=>vn(a,Vt,[]).push(Ne)),it.preStyleProps.forEach((Vt,Do)=>{if(Vt.size){let Tn=l.get(Do);Tn||l.set(Do,Tn=new Set),Vt.forEach((Ed,fp)=>Tn.add(fp))}}),it.postStyleProps.forEach((Vt,Do)=>{let Tn=c.get(Do);Tn||c.set(Do,Tn=new Set),Vt.forEach((Ed,fp)=>Tn.add(fp))})});if(pt.length){let K=[];pt.forEach(ne=>{K.push(jI(ne.triggerName,ne.errors))}),Xt.forEach(ne=>ne.destroy()),this.reportError(K)}let nt=new Map,fn=new Map;s.forEach(K=>{let ne=K.element;i.has(ne)&&(fn.set(ne,ne),this._beforeAnimationBuild(K.player.namespaceId,K.instruction,nt))}),r.forEach(K=>{let ne=K.element;this._getPreviousPlayers(ne,!1,K.namespaceId,K.triggerName,null).forEach(Ne=>{vn(nt,ne,[]).push(Ne),Ne.destroy()})});let wo=E.filter(K=>tk(K,l,c)),ks=new Map;JI(ks,this.driver,Z,c,mi).forEach(K=>{tk(K,l,c)&&wo.push(K)});let Co=new Map;m.forEach((K,ne)=>{JI(Co,this.driver,new Set(K),l,Tc)}),wo.forEach(K=>{let ne=ks.get(K),ae=Co.get(K);ks.set(K,new Map([...ne?.entries()??[],...ae?.entries()??[]]))});let up=[],G0=[],W0={};s.forEach(K=>{let{element:ne,player:ae,instruction:Ne}=K;if(i.has(ne)){if(u.has(ne)){ae.onDestroy(()=>hi(ne,Ne.toStyles)),ae.disabled=!0,ae.overrideTotalTime(Ne.totalTime),r.push(ae);return}let xt=W0;if(fn.size>1){let Cn=ne,Fr=[];for(;Cn=Cn.parentNode;){let it=fn.get(Cn);if(it){xt=it;break}Fr.push(Cn)}Fr.forEach(it=>fn.set(it,xt))}let Di=this._buildAnimation(ae.namespaceId,Ne,nt,o,Co,ks);if(ae.setRealPlayer(Di),xt===W0)up.push(ae);else{let Cn=this.playersByElement.get(xt);Cn&&Cn.length&&(ae.parentPlayer=Cr(Cn)),r.push(ae)}}else fo(ne,Ne.fromStyles),ae.onDestroy(()=>hi(ne,Ne.toStyles)),G0.push(ae),u.has(ne)&&r.push(ae)}),G0.forEach(K=>{let ne=o.get(K.element);if(ne&&ne.length){let ae=Cr(ne);K.setRealPlayer(ae)}}),r.forEach(K=>{K.parentPlayer?K.syncPlayerEvents(K.parentPlayer):K.destroy()});for(let K=0;K<E.length;K++){let ne=E[K],ae=ne[pi];if(Ea(ne,Lm),ae&&ae.hasAnimation)continue;let Ne=[];if(a.size){let Di=a.get(ne);Di&&Di.length&&Ne.push(...Di);let Cn=this.driver.query(ne,Bm,!0);for(let Fr=0;Fr<Cn.length;Fr++){let it=a.get(Cn[Fr]);it&&it.length&&Ne.push(...it)}}let xt=Ne.filter(Di=>!Di.destroyed);xt.length?KB(this,ne,xt):this.processLeaveNode(ne)}return E.length=0,up.forEach(K=>{this.players.push(K),K.onDone(()=>{K.destroy();let ne=this.players.indexOf(K);this.players.splice(ne,1)}),K.play()}),up}afterFlush(n){this._flushFns.push(n)}afterFlushAnimationsDone(n){this._whenQuietFns.push(n)}_getPreviousPlayers(n,e,i,r,o){let s=[];if(e){let a=this.playersByQueriedElement.get(n);a&&(s=a)}else{let a=this.playersByElement.get(n);if(a){let l=!o||o==Nc;a.forEach(c=>{c.queued||!l&&c.triggerName!=r||s.push(c)})}}return(i||r)&&(s=s.filter(a=>!(i&&i!=a.namespaceId||r&&r!=a.triggerName))),s}_beforeAnimationBuild(n,e,i){let r=e.triggerName,o=e.element,s=e.isRemovalTransition?void 0:n,a=e.isRemovalTransition?void 0:r;for(let l of e.timelines){let c=l.element,u=c!==o,f=vn(i,c,[]);this._getPreviousPlayers(c,u,s,a,e.toState).forEach(m=>{let _=m.getRealPlayer();_.beforeDestroy&&_.beforeDestroy(),m.destroy(),f.push(m)})}fo(o,e.fromStyles)}_buildAnimation(n,e,i,r,o,s){let a=e.triggerName,l=e.element,c=[],u=new Set,f=new Set,h=e.timelines.map(_=>{let D=_.element;u.add(D);let E=D[pi];if(E&&E.removedBeforeQueried)return new wr(_.duration,_.delay);let M=D!==l,Z=ZB((i.get(D)||GB).map(nt=>nt.getRealPlayer())).filter(nt=>{let fn=nt;return fn.element?fn.element===D:!1}),Te=o.get(D),Ee=s.get(D),Xt=Yy(this._normalizer,_.keyframes,Te,Ee),pt=this._buildPlayer(_,Xt,Z);if(_.subTimeline&&r&&f.add(D),M){let nt=new Lc(n,a,D);nt.setRealPlayer(pt),c.push(nt)}return pt});c.forEach(_=>{vn(this.playersByQueriedElement,_.element,[]).push(_),_.onDone(()=>qB(this.playersByQueriedElement,_.element,_))}),u.forEach(_=>Hn(_,tb));let m=Cr(h);return m.onDestroy(()=>{u.forEach(_=>Ea(_,tb)),hi(l,e.toStyles)}),f.forEach(_=>{vn(r,_,[]).push(m)}),m}_buildPlayer(n,e,i){return e.length>0?this.driver.animate(n.element,e,n.duration,n.delay,n.easing,i):new wr(n.duration,n.delay)}},Lc=class{namespaceId;triggerName;element;_player=new wr;_containsRealPlayer=!1;_queuedCallbacks=new Map;destroyed=!1;parentPlayer=null;markedForDestroy=!1;disabled=!1;queued=!0;totalTime=0;constructor(n,e,i){this.namespaceId=n,this.triggerName=e,this.element=i}setRealPlayer(n){this._containsRealPlayer||(this._player=n,this._queuedCallbacks.forEach((e,i)=>{e.forEach(r=>Nm(n,i,void 0,r))}),this._queuedCallbacks.clear(),this._containsRealPlayer=!0,this.overrideTotalTime(n.totalTime),this.queued=!1)}getRealPlayer(){return this._player}overrideTotalTime(n){this.totalTime=n}syncPlayerEvents(n){let e=this._player;e.triggerCallback&&n.onStart(()=>e.triggerCallback("start")),n.onDone(()=>this.finish()),n.onDestroy(()=>this.destroy())}_queueEvent(n,e){vn(this._queuedCallbacks,n,[]).push(e)}onDone(n){this.queued&&this._queueEvent("done",n),this._player.onDone(n)}onStart(n){this.queued&&this._queueEvent("start",n),this._player.onStart(n)}onDestroy(n){this.queued&&this._queueEvent("destroy",n),this._player.onDestroy(n)}init(){this._player.init()}hasStarted(){return this.queued?!1:this._player.hasStarted()}play(){!this.queued&&this._player.play()}pause(){!this.queued&&this._player.pause()}restart(){!this.queued&&this._player.restart()}finish(){this._player.finish()}destroy(){this.destroyed=!0,this._player.destroy()}reset(){!this.queued&&this._player.reset()}setPosition(n){this.queued||this._player.setPosition(n)}getPosition(){return this.queued?0:this._player.getPosition()}triggerCallback(n){let e=this._player;e.triggerCallback&&e.triggerCallback(n)}};function qB(t,n,e){let i=t.get(n);if(i){if(i.length){let r=i.indexOf(e);i.splice(r,1)}i.length==0&&t.delete(n)}return i}function QB(t){return t??null}function Hm(t){return t&&t.nodeType===1}function YB(t){return t=="start"||t=="done"}function XI(t,n){let e=t.style.display;return t.style.display=n??"none",e}function JI(t,n,e,i,r){let o=[];e.forEach(l=>o.push(XI(l)));let s=[];i.forEach((l,c)=>{let u=new Map;l.forEach(f=>{let h=n.computeStyle(c,f,r);u.set(f,h),(!h||h.length==0)&&(c[pi]=WB,s.push(c))}),t.set(c,u)});let a=0;return e.forEach(l=>XI(l,o[a++])),s}function ek(t,n){let e=new Map;if(t.forEach(a=>e.set(a,[])),n.length==0)return e;let i=1,r=new Set(n),o=new Map;function s(a){if(!a)return i;let l=o.get(a);if(l)return l;let c=a.parentNode;return e.has(c)?l=c:r.has(c)?l=i:l=s(c),o.set(a,l),l}return n.forEach(a=>{let l=s(a);l!==i&&e.get(l).push(a)}),e}function Hn(t,n){t.classList?.add(n)}function Ea(t,n){t.classList?.remove(n)}function KB(t,n,e){Cr(e).onDone(()=>t.processLeaveNode(n))}function ZB(t){let n=[];return ak(t,n),n}function ak(t,n){for(let e=0;e<t.length;e++){let i=t[e];i instanceof Ca?ak(i.players,n):n.push(i)}}function XB(t,n){let e=Object.keys(t),i=Object.keys(n);if(e.length!=i.length)return!1;for(let r=0;r<e.length;r++){let o=e[r];if(!n.hasOwnProperty(o)||t[o]!==n[o])return!1}return!0}function tk(t,n,e){let i=e.get(t);if(!i)return!1;let r=n.get(t);return r?i.forEach(o=>r.add(o)):n.set(t,i),e.delete(t),!0}var Sa=class{_driver;_normalizer;_transitionEngine;_timelineEngine;_triggerCache={};onRemovalComplete=(n,e)=>{};constructor(n,e,i){this._driver=e,this._normalizer=i,this._transitionEngine=new _b(n.body,e,i),this._timelineEngine=new pb(n.body,e,i),this._transitionEngine.onRemovalComplete=(r,o)=>this.onRemovalComplete(r,o)}registerTrigger(n,e,i,r,o){let s=n+"-"+r,a=this._triggerCache[s];if(!a){let l=[],c=[],u=ik(this._driver,o,l,c);if(l.length)throw TI(r,l);a=BB(r,u,this._normalizer),this._triggerCache[s]=a}this._transitionEngine.registerTrigger(e,r,a)}register(n,e){this._transitionEngine.register(n,e)}destroy(n,e){this._transitionEngine.destroy(n,e)}onInsert(n,e,i,r){this._transitionEngine.insertNode(n,e,i,r)}onRemove(n,e,i){this._transitionEngine.removeNode(n,e,i)}disableAnimations(n,e){this._transitionEngine.markElementAsDisabled(n,e)}process(n,e,i,r){if(i.charAt(0)=="@"){let[o,s]=Ky(i),a=r;this._timelineEngine.command(o,e,s,a)}else this._transitionEngine.trigger(n,e,i,r)}listen(n,e,i,r,o){if(i.charAt(0)=="@"){let[s,a]=Ky(i);return this._timelineEngine.listen(s,e,a,o)}return this._transitionEngine.listen(n,e,i,r,o)}flush(n=-1){this._transitionEngine.flush(n)}get players(){return[...this._transitionEngine.players,...this._timelineEngine.players]}whenRenderingDone(){return this._transitionEngine.whenRenderingDone()}afterFlushAnimationsDone(n){this._transitionEngine.afterFlushAnimationsDone(n)}};function JB(t,n){let e=null,i=null;return Array.isArray(n)&&n.length?(e=ab(n[0]),n.length>1&&(i=ab(n[n.length-1]))):n instanceof Map&&(e=ab(n)),e||i?new eV(t,e,i):null}var eV=(()=>{class t{_element;_startStyles;_endStyles;static initialStylesByElement=new WeakMap;_state=0;_initialStyles;constructor(e,i,r){this._element=e,this._startStyles=i,this._endStyles=r;let o=t.initialStylesByElement.get(e);o||t.initialStylesByElement.set(e,o=new Map),this._initialStyles=o}start(){this._state<1&&(this._startStyles&&hi(this._element,this._startStyles,this._initialStyles),this._state=1)}finish(){this.start(),this._state<2&&(hi(this._element,this._initialStyles),this._endStyles&&(hi(this._element,this._endStyles),this._endStyles=null),this._state=1)}destroy(){this.finish(),this._state<3&&(t.initialStylesByElement.delete(this._element),this._startStyles&&(fo(this._element,this._startStyles),this._endStyles=null),this._endStyles&&(fo(this._element,this._endStyles),this._endStyles=null),hi(this._element,this._initialStyles),this._state=3)}}return t})();function ab(t){let n=null;return t.forEach((e,i)=>{tV(i)&&(n=n||new Map,n.set(i,e))}),n}function tV(t){return t==="display"||t==="position"}var Km=class{element;keyframes;options;_specialStyles;_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_duration;_delay;_initialized=!1;_finished=!1;_started=!1;_destroyed=!1;_finalKeyframe;_originalOnDoneFns=[];_originalOnStartFns=[];domPlayer=null;time=0;parentPlayer=null;currentSnapshot=new Map;constructor(n,e,i,r){this.element=n,this.keyframes=e,this.options=i,this._specialStyles=r,this._duration=i.duration,this._delay=i.delay||0,this.time=this._duration+this._delay}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(n=>n()),this._onDoneFns=[])}init(){this._buildPlayer()&&this._preparePlayerBeforeStart()}_buildPlayer(){if(this._initialized)return this.domPlayer;this._initialized=!0;let n=this.keyframes,e=this._triggerWebAnimation(this.element,n,this.options);if(!e)return this._onFinish(),null;this.domPlayer=e,this._finalKeyframe=n.length?n[n.length-1]:new Map;let i=()=>this._onFinish();return e.addEventListener("finish",i),this.onDestroy(()=>{e.removeEventListener("finish",i)}),e}_preparePlayerBeforeStart(){this._delay?this._resetDomPlayerState():this.domPlayer?.pause()}_convertKeyframesToObject(n){let e=[];return n.forEach(i=>{e.push(Object.fromEntries(i))}),e}_triggerWebAnimation(n,e,i){let r=this._convertKeyframesToObject(e);try{return n.animate(r,i)}catch{return null}}onStart(n){this._originalOnStartFns.push(n),this._onStartFns.push(n)}onDone(n){this._originalOnDoneFns.push(n),this._onDoneFns.push(n)}onDestroy(n){this._onDestroyFns.push(n)}play(){let n=this._buildPlayer();n&&(this.hasStarted()||(this._onStartFns.forEach(e=>e()),this._onStartFns=[],this._started=!0,this._specialStyles&&this._specialStyles.start()),n.play())}pause(){this.init(),this.domPlayer?.pause()}finish(){this.init(),this.domPlayer&&(this._specialStyles&&this._specialStyles.finish(),this._onFinish(),this.domPlayer.finish())}reset(){this._resetDomPlayerState(),this._destroyed=!1,this._finished=!1,this._started=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}_resetDomPlayerState(){this.domPlayer?.cancel()}restart(){this.reset(),this.play()}hasStarted(){return this._started}destroy(){this._destroyed||(this._destroyed=!0,this._resetDomPlayerState(),this._onFinish(),this._specialStyles&&this._specialStyles.destroy(),this._onDestroyFns.forEach(n=>n()),this._onDestroyFns=[])}setPosition(n){this.domPlayer||this.init(),this.domPlayer&&(this.domPlayer.currentTime=n*this.time)}getPosition(){return this.domPlayer?+(this.domPlayer.currentTime??0)/this.time:this._initialized?1:0}get totalTime(){return this._delay+this._duration}beforeDestroy(){let n=new Map;this.hasStarted()&&this._finalKeyframe.forEach((i,r)=>{r!=="offset"&&n.set(r,this._finished?i:jm(this.element,r))}),this.currentSnapshot=n}triggerCallback(n){let e=n==="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Zm=class{validateStyleProperty(n){return!0}validateAnimatableStyleProperty(n){return!0}containsElement(n,e){return Zy(n,e)}getParentElement(n){return Pm(n)}query(n,e,i){return Xy(n,e,i)}computeStyle(n,e,i){return jm(n,e)}animate(n,e,i,r,o,s=[]){let a=r==0?"both":"forwards",l={duration:i,delay:r,fill:a};o&&(l.easing=o);let c=new Map,u=s.filter(m=>m instanceof Km);$I(i,r)&&u.forEach(m=>{m.currentSnapshot.forEach((_,D)=>c.set(D,_))});let f=UI(e).map(m=>new Map(m));f=GI(n,f,c);let h=JB(n,f);return new Km(n,f,l,h)}};var $m="@",lk="@.disabled",Xm=class{namespaceId;delegate;engine;_onDestroy;\u0275type=0;constructor(n,e,i,r){this.namespaceId=n,this.delegate=e,this.engine=i,this._onDestroy=r}get data(){return this.delegate.data}destroyNode(n){this.delegate.destroyNode?.(n)}destroy(){this.engine.destroy(this.namespaceId,this.delegate),this.engine.afterFlushAnimationsDone(()=>{queueMicrotask(()=>{this.delegate.destroy()})}),this._onDestroy?.()}createElement(n,e){return this.delegate.createElement(n,e)}createComment(n){return this.delegate.createComment(n)}createText(n){return this.delegate.createText(n)}appendChild(n,e){this.delegate.appendChild(n,e),this.engine.onInsert(this.namespaceId,e,n,!1)}insertBefore(n,e,i,r=!0){this.delegate.insertBefore(n,e,i),this.engine.onInsert(this.namespaceId,e,n,r)}removeChild(n,e,i,r){if(r){this.delegate.removeChild(n,e,i,r);return}this.parentNode(e)&&this.engine.onRemove(this.namespaceId,e,this.delegate)}selectRootElement(n,e){return this.delegate.selectRootElement(n,e)}parentNode(n){return this.delegate.parentNode(n)}nextSibling(n){return this.delegate.nextSibling(n)}setAttribute(n,e,i,r){this.delegate.setAttribute(n,e,i,r)}removeAttribute(n,e,i){this.delegate.removeAttribute(n,e,i)}addClass(n,e){this.delegate.addClass(n,e)}removeClass(n,e){this.delegate.removeClass(n,e)}setStyle(n,e,i,r){this.delegate.setStyle(n,e,i,r)}removeStyle(n,e,i){this.delegate.removeStyle(n,e,i)}setProperty(n,e,i){e.charAt(0)==$m&&e==lk?this.disableAnimations(n,!!i):this.delegate.setProperty(n,e,i)}setValue(n,e){this.delegate.setValue(n,e)}listen(n,e,i,r){return this.delegate.listen(n,e,i,r)}disableAnimations(n,e){this.engine.disableAnimations(n,e)}},vb=class extends Xm{factory;constructor(n,e,i,r,o){super(e,i,r,o),this.factory=n,this.namespaceId=e}setProperty(n,e,i){e.charAt(0)==$m?e.charAt(1)=="."&&e==lk?(i=i===void 0?!0:!!i,this.disableAnimations(n,i)):this.engine.process(this.namespaceId,n,e.slice(1),i):this.delegate.setProperty(n,e,i)}listen(n,e,i,r){if(e.charAt(0)==$m){let o=nV(n),s=e.slice(1),a="";return s.charAt(0)!=$m&&([s,a]=iV(s)),this.engine.listen(this.namespaceId,o,s,a,l=>{let c=l._data||-1;this.factory.scheduleListenerCallback(c,i,l)})}return this.delegate.listen(n,e,i,r)}};function nV(t){switch(t){case"body":return document.body;case"document":return document;case"window":return window;default:return t}}function iV(t){let n=t.indexOf("."),e=t.substring(0,n),i=t.slice(n+1);return[e,i]}var Jm=class{delegate;engine;_zone;_currentId=0;_microtaskId=1;_animationCallbacksBuffer=[];_rendererCache=new Map;_cdRecurDepth=0;constructor(n,e,i){this.delegate=n,this.engine=e,this._zone=i,e.onRemovalComplete=(r,o)=>{o?.removeChild(null,r)}}createRenderer(n,e){let r=this.delegate.createRenderer(n,e);if(!n||!e?.data?.animation){let c=this._rendererCache,u=c.get(r);if(!u){let f=()=>c.delete(r);u=new Xm("",r,this.engine,f),c.set(r,u)}return u}let o=e.id,s=e.id+"-"+this._currentId;this._currentId++,this.engine.register(s,n);let a=c=>{Array.isArray(c)?c.forEach(a):this.engine.registerTrigger(o,s,n,c.name,c)};return e.data.animation.forEach(a),new vb(this,s,r,this.engine)}begin(){this._cdRecurDepth++,this.delegate.begin&&this.delegate.begin()}_scheduleCountTask(){queueMicrotask(()=>{this._microtaskId++})}scheduleListenerCallback(n,e,i){if(n>=0&&n<this._microtaskId){this._zone.run(()=>e(i));return}let r=this._animationCallbacksBuffer;r.length==0&&queueMicrotask(()=>{this._zone.run(()=>{r.forEach(o=>{let[s,a]=o;s(a)}),this._animationCallbacksBuffer=[]})}),r.push([e,i])}end(){this._cdRecurDepth--,this._cdRecurDepth==0&&this._zone.runOutsideAngular(()=>{this._scheduleCountTask(),this.engine.flush(this._microtaskId)}),this.delegate.end&&this.delegate.end()}whenRenderingDone(){return this.engine.whenRenderingDone()}componentReplaced(n){this.engine.flush(),this.delegate.componentReplaced?.(n)}};var oV=(()=>{class t extends Sa{constructor(e,i,r){super(e,i,r)}ngOnDestroy(){this.flush()}static \u0275fac=function(i){return new(i||t)(V(G),V(ms),V(hs))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();function sV(){return new Gm}function aV(){return new Jm(d(cc),d(Sa),d(z))}var ck=[{provide:hs,useFactory:sV},{provide:Sa,useClass:oV},{provide:lt,useFactory:aV}],One=[{provide:ms,useClass:yb},{provide:io,useValue:"NoopAnimations"},...ck],lV=[{provide:ms,useFactory:()=>new Zm},{provide:io,useFactory:()=>"BrowserAnimations"},...ck];function dk(){return Vn("NgEagerAnimations"),[...lV]}var ps=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var wb;try{wb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{wb=!1}var be=(()=>{class t{_platformId=d(Xo);isBrowser=this._platformId?HE(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||wb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ia(t){return Array.isArray(t)?t:[t]}var uk=new Set,gs,ka=(()=>{class t{_platform=d(be);_nonce=d(Jo,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):dV}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&cV(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cV(t,n){if(!uk.has(t))try{gs||(gs=document.createElement("style"),n&&gs.setAttribute("nonce",n),gs.setAttribute("type","text/css"),document.head.appendChild(gs)),gs.sheet&&(gs.sheet.insertRule(`@media ${t} {body{ }}`,0),uk.add(t))}catch(e){console.error(e)}}function dV(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var _s=(()=>{class t{_mediaMatcher=d(ka);_zone=d(z);_queries=new Map;_destroySubject=new I;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return fk(Ia(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=fk(Ia(e)).map(s=>this._registerQuery(s).observable),o=hn(r);return o=Ur(o.pipe(rt(1)),o.pipe(hl(1),tr(0))),o.pipe(ee(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new de(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(St(i),ee(({matches:s})=>({query:e,matches:s})),ue(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function fk(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var Bc={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var fV=new b("MATERIAL_ANIMATIONS"),mk=null;function mV(){return d(fV,{optional:!0})?.animationsDisabled||d(io,{optional:!0})==="NoopAnimations"?"di-disabled":(mk??=d(ka).matchMedia("(prefers-reduced-motion)").matches,mk?"reduced-motion":"enabled")}function Ve(){return mV()!=="enabled"}function Vc(t){return t.buttons===0||t.detail===0}function jc(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Cb;function hk(){if(Cb==null){let t=typeof document<"u"?document.head:null;Cb=!!(t&&(t.createShadowRoot||t.attachShadow))}return Cb}function Db(t){if(hk()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Ma(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Lt(t){return t.composedPath?t.composedPath()[0]:t.target}var zc;function pk(){if(zc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>zc=!0}))}finally{zc=zc||!1}return zc}function Ta(t){return pk()?t:!!t.capture}function Yt(t,n=0){return eh(t)?Number(t):arguments.length===2?n:0}function eh(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function cn(t){return t instanceof O?t.nativeElement:t}var gk=new b("cdk-input-modality-detector-options"),_k={ignoreKeys:[18,17,224,91,16]},vk=650,xb={passive:!0,capture:!0},yk=(()=>{class t{_platform=d(be);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new qe(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Lt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<vk||(this._modality.next(Vc(e)?"keyboard":"mouse"),this._mostRecentTarget=Lt(e))};_onTouchstart=e=>{if(jc(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Lt(e)};constructor(){let e=d(z),i=d(G),r=d(gk,{optional:!0});if(this._options=w(w({},_k),r),this.modalityDetected=this._modality.pipe(hl(1)),this.modalityChanged=this.modalityDetected.pipe(su()),this._platform.isBrowser){let o=d(lt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,xb),o.listen(i,"mousedown",this._onMousedown,xb),o.listen(i,"touchstart",this._onTouchstart,xb)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Uc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Uc||{}),bk=new b("cdk-focus-monitor-default-options"),th=Ta({passive:!0,capture:!0}),Mn=(()=>{class t{_ngZone=d(z);_platform=d(be);_inputModalityDetector=d(yk);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(G);_stopInputModalityDetector=new I;constructor(){let e=d(bk,{optional:!0});this._detectionMode=e?.detectionMode||Uc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Lt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=cn(e);if(!this._platform.isBrowser||r.nodeType!==1)return q();let o=Db(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new I,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=cn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=cn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Uc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Uc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?vk:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Lt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,th),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,th)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(ue(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,th),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,th),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var nh=new WeakMap,tt=(()=>{class t{_appRef;_injector=d(Y);_environmentInjector=d(ze);load(e){let i=this._appRef=this._appRef||this._injector.get(pn),r=nh.get(i);r||(r={loaders:new Set,refs:[]},nh.set(i,r),i.onDestroy(()=>{nh.get(i)?.refs.forEach(o=>o.destroy()),nh.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(jf(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Aa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),ih;function hV(){if(ih===void 0&&(ih=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(ih=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return ih}function vs(t){return hV()?.createHTML(t)||t}function wk(t,n,e){let i=e.sanitize(bt.HTML,n);t.innerHTML=vs(i||"")}function pV(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var Ck=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Dk=(()=>{class t{_mutationObserverFactory=d(Ck);_observedElements=new Map;_ngZone=d(z);constructor(){}ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=cn(e);return new de(r=>{let s=this._observeElement(i).pipe(ee(a=>a.filter(l=>!pV(l))),Ie(a=>!!a.length)).subscribe(a=>{this._ngZone.run(()=>{r.next(a)})});return()=>{s.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new I,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),xk=(()=>{class t{_contentObserver=d(Dk);_elementRef=d(O);event=new Q;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Yt(e),this._subscribe()}_debounce;_currentSubscription=null;constructor(){}ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(tr(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",j],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})(),Ra=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({providers:[Ck]})}return t})();var Oa=(()=>{class t{_platform=d(be);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return _V(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=gV(EV(e));if(i&&(Ek(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=Ek(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!DV(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return xV(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function gV(t){try{return t.frameElement}catch{return null}}function _V(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function vV(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function yV(t){return wV(t)&&t.type=="hidden"}function bV(t){return CV(t)&&t.hasAttribute("href")}function wV(t){return t.nodeName.toLowerCase()=="input"}function CV(t){return t.nodeName.toLowerCase()=="a"}function kk(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function Ek(t){if(!kk(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function DV(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function xV(t){return yV(t)?!1:vV(t)||bV(t)||t.hasAttribute("contenteditable")||kk(t)}function EV(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var rh=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?Ke(n,{injector:this._injector}):setTimeout(n)}},Hc=(()=>{class t{_checker=d(Oa);_ngZone=d(z);_document=d(G);_injector=d(Y);constructor(){d(tt).load(Aa)}create(e,i=!1){return new rh(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Mk=new b("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Tk=new b("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),SV=0,$c=(()=>{class t{_ngZone=d(z);_defaultOptions=d(Tk,{optional:!0});_liveElement;_document=d(G);_sanitizer=d(fc);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(Mk,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:wk(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${SV++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var mo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(mo||{}),Sk="cdk-high-contrast-black-on-white",Ik="cdk-high-contrast-white-on-black",Eb="cdk-high-contrast-active",Ak=(()=>{class t{_platform=d(be);_hasCheckedHighContrastMode=!1;_document=d(G);_breakpointSubscription;constructor(){this._breakpointSubscription=d(_s).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return mo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return mo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return mo.BLACK_ON_WHITE}return mo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(Eb,Sk,Ik),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===mo.BLACK_ON_WHITE?e.add(Eb,Sk):i===mo.WHITE_ON_BLACK&&e.add(Eb,Ik)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Gc=(()=>{class t{constructor(){d(Ak)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Ra]})}return t})();var IV=200,oh=class{_letterKeyStream=new I;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new I;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:IV;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(st(e=>this._pressedLetters.push(e)),tr(n),Ie(()=>this._pressedLetters.length>0),ee(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Ot(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var sh=class{_items;_activeItemIndex=X(-1);_activeItem=X(null);_wrap=!1;_typeaheadSubscription=Se.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof cr?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):mr(n)&&(this._effectRef=lr(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new I;change=new I;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new oh(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Ot(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return mr(this._items)?this._items():this._items instanceof cr?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Wc=class extends sh{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var kb={},We=class t{_appId=d(no);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),kb.hasOwnProperty(n)||(kb[n]=0),`${n}${e?t._infix+"-":""}${kb[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Pk=" ";function Ab(t,n,e){let i=lh(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(Pk)))}function ch(t,n,e){let i=lh(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(Pk)):t.removeAttribute(n)}function lh(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var Lk="cdk-describedby-message",ah="cdk-describedby-host",Tb=0,dh=(()=>{class t{_platform=d(be);_document=d(G);_messageRegistry=new Map;_messagesContainer=null;_id=`${Tb++}`;constructor(){d(tt).load(Aa),this._id=d(no)+"-"+Tb++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Mb(i,r);typeof i!="string"?(Fk(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Mb(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${ah}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(ah);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");Fk(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Mb(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=lh(e,"aria-describedby").filter(r=>r.indexOf(Lk)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);Ab(e,"aria-describedby",r.messageElement.id),e.setAttribute(ah,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,ch(e,"aria-describedby",r.messageElement.id),e.removeAttribute(ah)}_isElementDescribedByMessage(e,i){let r=lh(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Mb(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function Fk(t,n){t.id||(t.id=`${Lk}-${n}-${Tb++}`)}var gi=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(gi||{}),uh,ys;function fh(){if(ys==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return ys=!1,ys;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)ys=!0;else{let t=Element.prototype.scrollTo;t?ys=!/\{\s*\[native code\]\s*\}/.test(t.toString()):ys=!1}}return ys}function Na(){if(typeof document!="object"||!document)return gi.NORMAL;if(uh==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),uh=gi.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,uh=t.scrollLeft===0?gi.NEGATED:gi.INVERTED),t.remove()}return uh}function Rb(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Fa,Bk=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ob(){if(Fa)return Fa;if(typeof document!="object"||!document)return Fa=new Set(Bk),Fa;let t=document.createElement("input");return Fa=new Set(Bk.filter(n=>(t.setAttribute("type",n),t.type===n))),Fa}function ht(t){return t==null?"":typeof t=="string"?t:`${t}px`}function ct(t){return t!=null&&`${t}`!="false"}var $n=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})($n||{}),Nb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=$n.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},Vk=Ta({passive:!0,capture:!0}),Fb=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,Vk)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,Vk)))}_delegateEventHandler=n=>{let e=Lt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},qc={enterDuration:225,exitDuration:150},kV=800,jk=Ta({passive:!0,capture:!0}),zk=["mousedown","touchstart"],Uk=["mouseup","mouseleave","touchend","touchcancel"],MV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),bs=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Fb;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=cn(i)),o&&o.get(tt).load(MV)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=w(w({},qc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||TV(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${l-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),h=f.transitionProperty,m=f.transitionDuration,_=h==="none"||m==="0s"||m==="0s, 0s"||r.width===0&&r.height===0,D=new Nb(this,u,i,_);u.style.transform="scale3d(1, 1, 1)",D.state=$n.FADING_IN,i.persistent||(this._mostRecentTransientRipple=D);let E=null;return!_&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let M=()=>{E&&(E.fallbackTimer=null),clearTimeout(Te),this._finishRippleTransition(D)},Z=()=>this._destroyRipple(D),Te=setTimeout(Z,c+100);u.addEventListener("transitionend",M),u.addEventListener("transitioncancel",Z),E={onTransitionEnd:M,onTransitionCancel:Z,fallbackTimer:Te}}),this._activeRipples.set(D,E),(_||!c)&&this._finishRippleTransition(D),D}fadeOutRipple(n){if(n.state===$n.FADING_OUT||n.state===$n.HIDDEN)return;let e=n.element,i=w(w({},qc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=$n.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=cn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,zk.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Uk.forEach(e=>{this._triggerElement.addEventListener(e,this,jk)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===$n.FADING_IN?this._startFadeOutTransition(n):n.state===$n.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=$n.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=$n.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Vc(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+kV;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!jc(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===$n.VISIBLE||n.config.terminateOnPointerUp&&n.state===$n.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(zk.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(Uk.forEach(e=>n.removeEventListener(e,this,jk)),this._pointerUpEventsRegistered=!1))}};function TV(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Qc=new b("mat-ripple-global-options"),Pa=(()=>{class t{_elementRef=d(O);_animationsDisabled=Ve();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(z),i=d(be),r=d(Qc,{optional:!0}),o=d(Y);this._globalOptions=r||{},this._rippleRenderer=new bs(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:w(w(w({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,w(w({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,w(w({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var Hk=(()=>{class t{_animationsDisabled=Ve();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&N("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var Gn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var AV=["text"],RV=[[["mat-icon"]],"*"],OV=["mat-icon","*"];function NV(t,n){if(t&1&&F(0,"mat-pseudo-checkbox",1),t&2){let e=W();k("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function FV(t,n){if(t&1&&F(0,"mat-pseudo-checkbox",3),t&2){let e=W();k("disabled",e.disabled)}}function PV(t,n){if(t&1&&(p(0,"span",4),y(1),g()),t&2){let e=W();v(),Oe("(",e.group.label,")")}}var Lb=new b("MAT_OPTION_PARENT_COMPONENT"),Bb=new b("MatOptgroup");var Pb=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},La=(()=>{class t{_element=d(O);_changeDetectorRef=d(Ce);_parent=d(Lb,{optional:!0});group=d(Bb,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(We).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=X(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new Q;_text;_stateChanges=new I;constructor(){let e=d(tt);e.load(Gn),e.load(Aa),this._signalDisableRipple=!!this._parent&&mr(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ot(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Pb(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&He(AV,7),i&2){let o;H(o=$())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&A("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(qt("id",r.id),te("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),N("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",j]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:OV,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(xe(RV),me(0,NV,1,2,"mat-pseudo-checkbox",1),U(1),p(2,"span",2,0),U(4,1),g(),me(5,FV,1,1,"mat-pseudo-checkbox",3),me(6,PV,2,1,"span",4),F(7,"div",5)),i&2&&(he(r.multiple?0:-1),v(5),he(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),v(),he(r.group&&r.group._inert?6:-1),v(),k("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[Hk,Pa],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function $k(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function Gk(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var LV=new b("cdk-dir-doc",{providedIn:"root",factory:()=>d(G)}),BV=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function Wk(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?BV.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Dt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=X("ltr");change=new Q;constructor(){let e=d(LV,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(Wk(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _e=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();var Ba=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var mh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var Vb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Ba,mh,La,_e]})}return t})();var VV={capture:!0},jV=["focus","mousedown","mouseenter","touchstart"],jb="mat-ripple-loader-uninitialized",zb="mat-ripple-loader-class-name",qk="mat-ripple-loader-centered",hh="mat-ripple-loader-disabled",Qk=(()=>{class t{_document=d(G);_animationsDisabled=Ve();_globalRippleOptions=d(Qc,{optional:!0});_platform=d(be);_ngZone=d(z);_injector=d(Y);_eventCleanups;_hosts=new Map;constructor(){let e=d(lt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>jV.map(i=>e.listen(this._document,i,this._onInteraction,VV)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(jb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(zb))&&e.setAttribute(zb,i.className||""),i.centered&&e.setAttribute(qk,""),i.disabled&&e.setAttribute(hh,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(hh,""):e.removeAttribute(hh)}_onInteraction=e=>{let i=Lt(e);if(i instanceof HTMLElement){let r=i.closest(`[${jb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(zb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??qc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??qc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(hh),rippleConfig:{centered:e.hasAttribute(qk),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new bs(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(jb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Va=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var zV=["mat-internal-form-field",""],UV=["*"],ph=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&N("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:zV,ngContentSelectors:UV,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var Dr={production:!0,apiUrl:"https://auth-project.runasp.net"};var bn=class t{constructor(n){this.http=n}apiUrl=`${Dr.apiUrl}/api/auth`;tokenKey="jwt_token";isAuthenticatedSubject=new qe(this.hasToken());register(n){return this.http.post(`${this.apiUrl}/register`,n)}login(n){return this.http.post(`${this.apiUrl}/login`,n).pipe(st(e=>{e.token&&this.setToken(e.token)}))}verifyOtp(n){return this.http.post(`${this.apiUrl}/verify-otp`,n).pipe(st(e=>{e.token&&this.setToken(e.token)}))}forgotPassword(n){return this.http.post(`${this.apiUrl}/forgot-password`,n)}verifyResetOtp(n){return this.http.post(`${this.apiUrl}/verify-reset-otp`,n)}resetPassword(n){return this.http.post(`${this.apiUrl}/reset-password`,n)}logout(){localStorage.removeItem(this.tokenKey),this.isAuthenticatedSubject.next(!1)}getToken(){return localStorage.getItem(this.tokenKey)}hasToken(){return!!this.getToken()}setToken(n){localStorage.setItem(this.tokenKey,n),this.isAuthenticatedSubject.next(!0)}isAuthenticated(){return this.isAuthenticatedSubject.asObservable()}static \u0275fac=function(e){return new(e||t)(V(ji))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Yk=()=>{let t=d(bn),n=d(mt);return t.getToken()?!0:n.createUrlTree(["/login"])};var iM=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(R($e),R(O))};static \u0275dir=S({type:t})}return t})(),rM=(()=>{class t extends iM{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,features:[J]})}return t})(),ws=new b("");var HV={provide:ws,useExisting:kt(()=>Wn),multi:!0};function $V(){let t=jn()?jn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var GV=new b(""),Wn=(()=>{class t extends iM{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!$V())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(R($e),R(O),R(GV,8))};static \u0275dir=S({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&A("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[ge([HV]),J]})}return t})();function Wb(t){return t==null||qb(t)===0}function qb(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Er=new b(""),Qb=new b(""),WV=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,le=class{static min(n){return oM(n)}static max(n){return qV(n)}static required(n){return QV(n)}static requiredTrue(n){return YV(n)}static email(n){return KV(n)}static minLength(n){return ZV(n)}static maxLength(n){return sM(n)}static pattern(n){return XV(n)}static nullValidator(n){return yh()}static compose(n){return fM(n)}static composeAsync(n){return mM(n)}};function oM(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function qV(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function QV(t){return Wb(t.value)?{required:!0}:null}function YV(t){return t.value===!0?null:{required:!0}}function KV(t){return Wb(t.value)||WV.test(t.value)?null:{email:!0}}function ZV(t){return n=>{let e=n.value?.length??qb(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function sM(t){return n=>{let e=n.value?.length??qb(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function XV(t){if(!t)return yh;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Wb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function yh(t){return null}function aM(t){return t!=null}function lM(t){return ro(t)?Ge(t):t}function cM(t){let n={};return t.forEach(e=>{n=e!=null?w(w({},n),e):n}),Object.keys(n).length===0?null:n}function dM(t,n){return n.map(e=>e(t))}function JV(t){return!t.validate}function uM(t){return t.map(n=>JV(n)?n:e=>n.validate(e))}function fM(t){if(!t)return null;let n=t.filter(aM);return n.length==0?null:function(e){return cM(dM(e,n))}}function Yb(t){return t!=null?fM(uM(t)):null}function mM(t){if(!t)return null;let n=t.filter(aM);return n.length==0?null:function(e){let i=dM(e,n).map(lM);return fl(i).pipe(ee(cM))}}function Kb(t){return t!=null?mM(uM(t)):null}function Kk(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function hM(t){return t._rawValidators}function pM(t){return t._rawAsyncValidators}function Ub(t){return t?Array.isArray(t)?t:[t]:[]}function bh(t,n){return Array.isArray(t)?t.includes(n):t===n}function Zk(t,n){let e=Ub(n);return Ub(t).forEach(r=>{bh(e,r)||e.push(r)}),e}function Xk(t,n){return Ub(n).filter(e=>!bh(t,e))}var wh=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Yb(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Kb(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},ho=class extends wh{name;get formDirective(){return null}get path(){return null}},xr=class extends wh{_parent=null;name=null;valueAccessor=null},Ch=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Gi=(()=>{class t extends Ch{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(R(xr,2))};static \u0275dir=S({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&N("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[J]})}return t})(),Wi=(()=>{class t extends Ch{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(R(ho,10))};static \u0275dir=S({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&N("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[J]})}return t})();var Zc="VALID",_h="INVALID",ja="PENDING",Xc="DISABLED",po=class{},Dh=class extends po{value;source;constructor(n,e){super(),this.value=n,this.source=e}},ed=class extends po{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},td=class extends po{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},za=class extends po{status;source;constructor(n,e){super(),this.status=n,this.source=e}},xh=class extends po{source;constructor(n){super(),this.source=n}},nd=class extends po{source;constructor(n){super(),this.source=n}};function Zb(t){return(Ih(t)?t.validators:t)||null}function ej(t){return Array.isArray(t)?Yb(t):t||null}function Xb(t,n){return(Ih(n)?n.asyncValidators:t)||null}function tj(t){return Array.isArray(t)?Kb(t):t||null}function Ih(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function gM(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new x(1e3,"");if(!i[e])throw new x(1001,"")}function _M(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new x(-1002,"")})}var Ua=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Le(this.statusReactive)}set status(n){Le(()=>this.statusReactive.set(n))}_status=sn(()=>this.statusReactive());statusReactive=X(void 0);get valid(){return this.status===Zc}get invalid(){return this.status===_h}get pending(){return this.status===ja}get disabled(){return this.status===Xc}get enabled(){return this.status!==Xc}errors;get pristine(){return Le(this.pristineReactive)}set pristine(n){Le(()=>this.pristineReactive.set(n))}_pristine=sn(()=>this.pristineReactive());pristineReactive=X(!0);get dirty(){return!this.pristine}get touched(){return Le(this.touchedReactive)}set touched(n){Le(()=>this.touchedReactive.set(n))}_touched=sn(()=>this.touchedReactive());touchedReactive=X(!1);get untouched(){return!this.touched}_events=new I;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(Zk(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(Zk(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Xk(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Xk(n,this._rawAsyncValidators))}hasValidator(n){return bh(this._rawValidators,n)}hasAsyncValidator(n){return bh(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(oe(w({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new td(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new td(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(oe(w({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ed(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new ed(!0,i))}markAsPending(n={}){this.status=ja;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new za(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(oe(w({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Xc,this.errors=null,this._forEachChild(r=>{r.disable(oe(w({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Dh(this.value,i)),this._events.next(new za(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(oe(w({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Zc,this._forEachChild(i=>{i.enable(oe(w({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(oe(w({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Zc||this.status===ja)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Dh(this.value,e)),this._events.next(new za(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(oe(w({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Xc:Zc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=ja,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=lM(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new za(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new Q,this.statusChanges=new Q}_calculateStatus(){return this._allControlsDisabled()?Xc:this.errors?_h:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ja)?ja:this._anyControlsHaveStatus(_h)?_h:Zc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new ed(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new td(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Ih(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=ej(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=tj(this._rawAsyncValidators)}},Ha=class extends Ua{constructor(n,e,i){super(Zb(e),Xb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){_M(this,!0,n),Object.keys(n).forEach(i=>{gM(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,oe(w({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new nd(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Hb=class extends Ha{};var Jb=new b("",{factory:()=>e0}),e0="always";function nj(t,n){return[...n.path,t]}function $b(t,n,e=e0){t0(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),rj(t,n),sj(t,n),oj(t,n),ij(t,n)}function Jk(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Sh(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Eh(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function ij(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function t0(t,n){let e=hM(t);n.validator!==null?t.setValidators(Kk(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=pM(t);n.asyncValidator!==null?t.setAsyncValidators(Kk(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Eh(n._rawValidators,r),Eh(n._rawAsyncValidators,r)}function Sh(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=hM(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=pM(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Eh(n._rawValidators,i),Eh(n._rawAsyncValidators,i),e}function rj(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&vM(t,n)})}function oj(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&vM(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function vM(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function sj(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function yM(t,n){t==null,t0(t,n)}function aj(t,n){return Sh(t,n)}function lj(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function cj(t){return Object.getPrototypeOf(t.constructor)===rM}function bM(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function dj(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Wn?e=o:cj(o)?i=o:r=o}),r||i||e||null}function uj(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var fj={provide:ho,useExisting:kt(()=>id)},Jc=Promise.resolve(),id=(()=>{class t extends ho{callSetDisabledState;get submitted(){return Le(this.submittedReactive)}_submitted=sn(()=>this.submittedReactive());submittedReactive=X(!1);_directives=new Set;form;ngSubmit=new Q;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Ha({},Yb(e),Kb(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Jc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),$b(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Jc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Jc.then(()=>{let i=this._findContainer(e.path),r=new Ha({});yM(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Jc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Jc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),bM(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new xh(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(R(Er,10),R(Qb,10),R(Jb,8))};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&A("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ge([fj]),J]})}return t})();function eM(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function tM(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var vh=class extends Ua{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(Zb(e),Xb(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Ih(e)&&(e.nonNullable||e.initialValueIsDefault)&&(tM(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new nd(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){eM(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){eM(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){tM(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var mj=t=>t instanceof vh;var qi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})(),hj={provide:ws,useExisting:kt(()=>n0),multi:!0},n0=(()=>{class t extends rM{writeValue(e){let i=e??"";this.setProperty("value",i)}registerOnChange(e){this.onChange=i=>{e(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&A("input",function(s){return r.onChange(s.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[ge([hj]),J]})}return t})();var Gb=class extends Ua{constructor(n,e,i){super(Zb(e),Xb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){_M(this,!1,n),n.forEach((i,r)=>{gM(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],oe(w({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new nd(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var pj=(()=>{class t extends ho{callSetDisabledState;get submitted(){return Le(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=sn(()=>this._submittedReactive());_submittedReactive=X(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Sh(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return $b(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){Jk(e.control||null,e,!1),uj(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,bM(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new xh(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(Jk(i||null,e),mj(r)&&($b(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);yM(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&aj(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){t0(this.form,this),this._oldForm&&Sh(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(R(Er,10),R(Qb,10),R(Jb,8))};static \u0275dir=S({type:t,features:[J,Pe]})}return t})();var wM=new b("");var gj={provide:xr,useExisting:kt(()=>_i)},_i=(()=>{class t extends xr{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new Q;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=dj(this,o)}ngOnChanges(e){this._added||this._setUpControl(),lj(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return nj(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(R(ho,13),R(Er,10),R(Qb,10),R(ws,10),R(wM,8))};static \u0275dir=S({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[ge([gj]),J,Pe]})}return t})();var _j={provide:ho,useExisting:kt(()=>dn)},dn=(()=>{class t extends pj{form=null;ngSubmit=new Q;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&A("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ge([_j]),J]})}return t})();function vj(t){return typeof t=="number"?t:parseInt(t,10)}function yj(t){return typeof t=="number"?t:parseFloat(t)}var CM=(()=>{class t{_validator=yh;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):yh,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,features:[Pe]})}return t})();var bj={provide:Er,useExisting:kt(()=>i0),multi:!0},i0=(()=>{class t extends CM{min;inputName="min";normalizeInput=e=>yj(e);createValidator=e=>oM(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["input","type","number","min","","formControlName",""],["input","type","number","min","","formControl",""],["input","type","number","min","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&te("min",r._enabled?r.min:null)},inputs:{min:"min"},standalone:!1,features:[ge([bj]),J]})}return t})();var wj={provide:Er,useExisting:kt(()=>rd),multi:!0},rd=(()=>{class t extends CM{maxlength;inputName="maxlength";normalizeInput=e=>vj(e);createValidator=e=>sM(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","maxlength","","formControlName",""],["","maxlength","","formControl",""],["","maxlength","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&te("maxlength",r._enabled?r.maxlength:null)},inputs:{maxlength:"maxlength"},standalone:!1,features:[ge([wj]),J]})}return t})();var Cj=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();function nM(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var Qi=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return nM(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Ha(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Hb(r,i)}control(e,i,r){let o={};return this.useNonNullable?(nM(i)?o=i:(o.validators=i,o.asyncValidators=r),new vh(e,oe(w({},o),{nonNullable:!0}))):new vh(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new Gb(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof vh)return e;if(e instanceof Ua)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Yi=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:wM,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:Jb,useValue:e.callSetDisabledState??e0}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Cj]})}return t})();var Dj=["*"];var xj=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],Ej=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],Sj=new b("MAT_CARD_CONFIG"),Ki=(()=>{class t{appearance;constructor(){let e=d(Sj,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&N("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:Dj,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})(),DM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var kh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var xM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:Ej,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(xe(xj),U(0),Tt(1,"div",0),U(2,1),At(),U(3,2))},encapsulation:2,changeDetection:0})}return t})();var Zi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var r0=class{_box;_destroyed=new I;_resizeSubject=new I;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new de(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ie(e=>e.some(i=>i.target===n)),cu({bufferSize:1,refCount:!0}),ue(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},EM=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(z);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new r0(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ij=["notch"],kj=["matFormFieldNotchedOutline",""],Mj=["*"],SM=["iconPrefixContainer"],IM=["textPrefixContainer"],kM=["iconSuffixContainer"],MM=["textSuffixContainer"],Tj=["textField"],Aj=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],Rj=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function Oj(t,n){t&1&&F(0,"span",21)}function Nj(t,n){if(t&1&&(p(0,"label",20),U(1,1),me(2,Oj,1,0,"span",21),g()),t&2){let e=W(2);k("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),te("for",e._control.disableAutomaticLabeling?null:e._control.id),v(2),he(!e.hideRequiredMarker&&e._control.required?2:-1)}}function Fj(t,n){if(t&1&&me(0,Nj,3,5,"label",20),t&2){let e=W();he(e._hasFloatingLabel()?0:-1)}}function Pj(t,n){t&1&&F(0,"div",7)}function Lj(t,n){}function Bj(t,n){if(t&1&&se(0,Lj,0,0,"ng-template",13),t&2){W(2);let e=gt(1);k("ngTemplateOutlet",e)}}function Vj(t,n){if(t&1&&(p(0,"div",9),me(1,Bj,1,1,null,13),g()),t&2){let e=W();k("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),v(),he(e._forceDisplayInfixLabel()?-1:1)}}function jj(t,n){t&1&&(p(0,"div",10,2),U(2,2),g())}function zj(t,n){t&1&&(p(0,"div",11,3),U(2,3),g())}function Uj(t,n){}function Hj(t,n){if(t&1&&se(0,Uj,0,0,"ng-template",13),t&2){W();let e=gt(1);k("ngTemplateOutlet",e)}}function $j(t,n){t&1&&(p(0,"div",14,4),U(2,4),g())}function Gj(t,n){t&1&&(p(0,"div",15,5),U(2,5),g())}function Wj(t,n){t&1&&F(0,"div",16)}function qj(t,n){t&1&&(p(0,"div",18),U(1,6),g())}function Qj(t,n){if(t&1&&(p(0,"mat-hint",22),y(1),g()),t&2){let e=W(2);k("id",e._hintLabelId),v(),Pt(e.hintLabel)}}function Yj(t,n){if(t&1&&(p(0,"div",19),me(1,Qj,2,2,"mat-hint",22),U(2,7),F(3,"div",23),U(4,8),g()),t&2){let e=W();v(),he(e.hintLabel?1:-1)}}var un=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-label"]]})}return t})(),PM=new b("MatError"),Sr=(()=>{class t{id=d(We).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&qt("id",r.id)},inputs:{id:"id"},features:[ge([{provide:PM,useExisting:t}])]})}return t})(),o0=(()=>{class t{align="start";id=d(We).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(qt("id",r.id),te("align",null),N("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),LM=new b("MatPrefix"),s0=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matPrefix",""],["","matIconPrefix",""],["","matTextPrefix",""]],inputs:{_isTextSelector:[0,"matTextPrefix","_isTextSelector"]},features:[ge([{provide:LM,useExisting:t}])]})}return t})(),BM=new b("MatSuffix"),a0=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[ge([{provide:BM,useExisting:t}])]})}return t})(),VM=new b("FloatingLabelParent"),TM=(()=>{class t{_elementRef=d(O);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(EM);_ngZone=d(z);_parent=d(VM);_resizeSubscription=new Se;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return Kj(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&N("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function Kj(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var AM="mdc-line-ripple--active",Mh="mdc-line-ripple--deactivating",RM=(()=>{class t{_elementRef=d(O);_cleanupTransitionEnd;constructor(){let e=d(z),i=d($e);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Mh),e.add(AM)}deactivate(){this._elementRef.nativeElement.classList.add(Mh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Mh);e.propertyName==="opacity"&&r&&i.remove(AM,Mh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),OM=(()=>{class t{_elementRef=d(O);_ngZone=d(z);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&He(Ij,5),i&2){let o;H(o=$())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&N("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:kj,ngContentSelectors:Mj,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(xe(),gn(0,"div",1),Tt(1,"div",2,0),U(3),At(),gn(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),ad=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})();var ld=new b("MatFormField"),Zj=new b("MAT_FORM_FIELD_DEFAULT_OPTIONS"),NM="fill",Xj="auto",FM="fixed",Jj="translateY(-50%)",Kt=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Ce);_platform=d(be);_idGenerator=d(We);_ngZone=d(z);_defaults=d(Zj,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=nc("iconPrefixContainer");_textPrefixContainerSignal=nc("textPrefixContainer");_iconSuffixContainerSignal=nc("iconSuffixContainer");_textSuffixContainerSignal=nc("textSuffixContainer");_prefixSuffixContainers=sn(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=ME(un);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=ct(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||Xj}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||NM;this._appearanceSignal.set(i)}_appearanceSignal=X(NM);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||FM}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||FM}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new I;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ve();constructor(){let e=this._defaults,i=d(Dt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),lr(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=sn(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(St([void 0,void 0]),ee(()=>[i.errorState,i.userAriaDescribedBy]),lu(),Ie(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(ue(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Et(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){RE({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=sn(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,m=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,_=`var(--mat-mdc-form-field-label-transform, ${Jj} translateX(${m}))`,D=s+a+l+c;return[_,D]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Af(o,r._labelChild,un,5),wt(o,ad,5)(o,LM,5)(o,BM,5)(o,PM,5)(o,o0,5)),i&2){Of();let s;H(s=$())&&(r._formFieldControl=s.first),H(s=$())&&(r._prefixChildren=s),H(s=$())&&(r._suffixChildren=s),H(s=$())&&(r._errorChildren=s),H(s=$())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Rf(r._iconPrefixContainerSignal,SM,5)(r._textPrefixContainerSignal,IM,5)(r._iconSuffixContainerSignal,kM,5)(r._textSuffixContainerSignal,MM,5),He(Tj,5)(SM,5)(IM,5)(kM,5)(MM,5)(TM,5)(OM,5)(RM,5)),i&2){Of(4);let o;H(o=$())&&(r._textField=o.first),H(o=$())&&(r._iconPrefixContainer=o.first),H(o=$())&&(r._textPrefixContainer=o.first),H(o=$())&&(r._iconSuffixContainer=o.first),H(o=$())&&(r._textSuffixContainer=o.first),H(o=$())&&(r._floatingLabel=o.first),H(o=$())&&(r._notchedOutline=o.first),H(o=$())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&N("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ge([{provide:ld,useExisting:t},{provide:VM,useExisting:t}])],ngContentSelectors:Rj,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(xe(Aj),se(0,Fj,1,1,"ng-template",null,0,Lf),p(2,"div",6,1),A("click",function(s){return r._control.onContainerClick(s)}),me(4,Pj,1,0,"div",7),p(5,"div",8),me(6,Vj,2,2,"div",9),me(7,jj,3,0,"div",10),me(8,zj,3,0,"div",11),p(9,"div",12),me(10,Hj,1,1,null,13),U(11),g(),me(12,$j,3,0,"div",14),me(13,Gj,3,0,"div",15),g(),me(14,Wj,1,0,"div",16),g(),p(15,"div",17),me(16,qj,2,0,"div",18)(17,Yj,5,1,"div",19),g()),i&2){let o;v(2),N("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),v(2),he(!r._hasOutline()&&!r._control.disabled?4:-1),v(2),he(r._hasOutline()?6:-1),v(),he(r._hasIconPrefix?7:-1),v(),he(r._hasTextPrefix?8:-1),v(2),he(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),v(2),he(r._hasTextSuffix?12:-1),v(),he(r._hasIconSuffix?13:-1),v(),he(r._hasOutline()?-1:14),v(),N("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();v(),he((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[TM,OM,$v,RM,o0],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var Bt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Ra,Kt,_e]})}return t})();var ez=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),tz={passive:!0},jM=(()=>{class t{_platform=d(be);_ngZone=d(z);_renderer=d(lt).createRenderer(null,null);_styleLoader=d(tt);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ut;this._styleLoader.load(ez);let i=cn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new I,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,tz)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=cn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var zM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();var UM=new b("MAT_INPUT_VALUE_ACCESSOR");var nz=["button","checkbox","file","hidden","image","radio","range","reset","submit"],iz=new b("MAT_INPUT_CONFIG"),qn=(()=>{class t{_elementRef=d(O);_platform=d(be);ngControl=d(xr,{optional:!0,self:!0});_autofillMonitor=d(jM);_ngZone=d(z);_formField=d(ld,{optional:!0});_renderer=d($e);_uid=d(We).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(iz,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new I;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=ct(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(le.required)??!1}set required(e){this._required=ct(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Ob().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=ct(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Ob().has(e));constructor(){let e=d(id,{optional:!0}),i=d(dn,{optional:!0}),r=d(ps),o=d(UM,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?mr(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Va(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&lr(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){nz.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&A("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(qt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),te("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),N("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",j]},exportAs:["matInput"],features:[ge([{provide:ad,useExisting:t}]),Pe]})}return t})(),Qn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Bt,Bt,zM,_e]})}return t})();var rz=["mat-icon-button",""],oz=["*"],sz=new b("MAT_BUTTON_CONFIG");function HM(t){return t==null?void 0:an(t)}var l0=(()=>{class t{_elementRef=d(O);_ngZone=d(z);_animationsDisabled=Ve();_config=d(sz,{optional:!0});_focusMonitor=d(Mn);_cleanupClick;_renderer=d($e);_rippleLoader=d(Qk);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(tt).load(Gn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(te("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),on(r.color?"mat-"+r.color:""),N("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",j],disabled:[2,"disabled","disabled",j],ariaDisabled:[2,"aria-disabled","ariaDisabled",j],disabledInteractive:[2,"disabledInteractive","disabledInteractive",j],tabIndex:[2,"tabIndex","tabIndex",HM],_tabindex:[2,"tabindex","_tabindex",HM]}})}return t})(),Cs=(()=>{class t extends l0{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[J],attrs:rz,ngContentSelectors:oz,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(xe(),gn(0,"span",0),U(1),gn(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var az=["matButton",""],lz=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],cz=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var $M=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),wn=(()=>{class t extends l0{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=dz(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?$M.get(this._appearance):null,o=$M.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[J],attrs:az,ngContentSelectors:cz,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(xe(lz),gn(0,"span",0),U(1),Tt(2,"span",1),U(3,1),At(),U(4,2),gn(5,"span",2)(6,"span",3)),i&2&&N("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function dz(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Nt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Ba,_e]})}return t})();var cd=class{};function dd(t){return t&&typeof t.connect=="function"&&!(t instanceof sl)}var vi=(function(t){return t[t.REPLACED=0]="REPLACED",t[t.INSERTED=1]="INSERTED",t[t.MOVED=2]="MOVED",t[t.REMOVED=3]="REMOVED",t})(vi||{}),Th=class{viewCacheSize=20;_viewCache=[];applyChanges(n,e,i,r,o){n.forEachOperation((s,a,l)=>{let c,u;if(s.previousIndex==null){let f=()=>i(s,a,l);c=this._insertView(f,l,e,r(s)),u=c?vi.INSERTED:vi.REPLACED}else l==null?(this._detachAndCacheView(a,e),u=vi.REMOVED):(c=this._moveView(a,l,e,r(s)),u=vi.MOVED);o&&o({context:c?.context,operation:u,record:s})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let s=n();return i.createEmbeddedView(s.templateRef,s.context,s.index)}_detachAndCacheView(n,e){let i=e.detach(n);this._maybeCacheView(i,e)}_moveView(n,e,i,r){let o=i.get(n);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(n,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let i=e.indexOf(n);i===-1?n.destroy():e.remove(i)}}_insertViewFromCache(n,e){let i=this._viewCache.pop();return i&&e.insert(i,n),i||null}};var uz=20,Ir=(()=>{class t{_ngZone=d(z);_platform=d(be);_renderer=d(lt).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new I;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=uz){return this._platform.isBrowser?new de(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Bs(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):q()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ie(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=cn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),kr=(()=>{class t{elementRef=d(O);scrollDispatcher=d(Ir);ngZone=d(z);dir=d(Dt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new I;_renderer=d($e);_cleanupScroll;_elementScrolled=new I;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&Na()!=gi.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),Na()==gi.INVERTED?e.left=e.right:Na()==gi.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;fh()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&Na()==gi.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&Na()==gi.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),fz=20,Yn=(()=>{class t{_platform=d(be);_listeners;_viewportSize=null;_change=new I;_document=d(G);constructor(){let e=d(z),i=d(lt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=fz){return e>0?this._change.pipe(Bs(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var GM=new b("CDK_VIRTUAL_SCROLL_VIEWPORT");var Xi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})(),ud=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e,Xi,_e,Xi]})}return t})();var fd=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},yi=class extends fd{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Mr=class extends fd{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},c0=class extends fd{element;constructor(n){super(),this.element=n instanceof O?n.nativeElement:n}},go=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof yi)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Mr)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof c0)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Ah=class extends go{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Pi,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||Y.NULL,o=r.get(ze,i.injector);e=jf(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var _o=(()=>{class t extends go{_moduleRef=d(Pi,{optional:!0});_document=d(G);_viewContainerRef=d(ot);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new Q;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[J]})}return t})(),Tr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({})}return t})();var WM=fh();function Qa(t){return new Rh(t.get(Yn),t.get(G))}var Rh=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=ht(-this._previousScrollPosition.left),n.style.top=ht(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),WM&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),WM&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function JM(t,n){return new Oh(t.get(Ir),t.get(z),t.get(Yn),n)}var Oh=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ie(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var md=class{enable(){}disable(){}attach(){}};function d0(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function qM(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Es(t,n){return new Nh(t.get(Ir),t.get(Yn),t.get(z),n)}var Nh=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();d0(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},eT=(()=>{class t{_injector=d(Y);constructor(){}noop=()=>new md;close=e=>JM(this._injector,e);block=()=>Qa(this._injector);reposition=e=>Es(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ar=class{positionStrategy;scrollStrategy=new md;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Fh=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var tT=(()=>{class t{_attachedOverlays=[];_document=d(G);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),nT=(()=>{class t extends tT{_ngZone=d(z);_renderer=d(lt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),iT=(()=>{class t extends tT{_platform=d(be);_ngZone=d(z);_renderer=d(lt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Lt(e)};_clickListener=e=>{let i=Lt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(QM(a.overlayElement,i)||QM(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function QM(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var rT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Bh=(()=>{class t{_platform=d(be);_containerElement;_document=d(G);_styleLoader=d(tt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Rb()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Rb()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(rT)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),u0=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function f0(t){return t&&t.nodeType===1}var Wa=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new I;_attachments=new I;_detachments=new I;_positionStrategy;_scrollStrategy;_locationChanges=Se.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new I;_outsidePointerEvents=new I;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,u=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Ke(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=w(w({},this._config),n),this._updateElementSize()}setDirection(n){this._config=oe(w({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=ht(this._config.width),n.height=ht(this._config.height),n.minWidth=ht(this._config.minWidth),n.minHeight=ht(this._config.minHeight),n.maxWidth=ht(this._config.maxWidth),n.maxHeight=ht(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;f0(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new u0(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Ia(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Ke(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},YM="cdk-overlay-connected-position-bounding-box",hz=/([A-Za-z%]+)$/;function hd(t,n){return new Ph(n,t.get(Yn),t.get(G),t.get(be),t.get(Bh))}var Ph=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new I;_resizeSubscription=Se.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(YM),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),u=this._getOverlayFit(c,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&xs(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(YM),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof O?this._origin.nativeElement:f0(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=ZM(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let u=0-s,f=s+o.width-i.width,h=0-a,m=a+o.height-i.height,_=this._subtractOverflows(o.width,u,f),D=this._subtractOverflows(o.height,h,m),E=_*D;return{visibleArea:E,isCompletelyWithinViewport:o.width*o.height===E,fitsInViewportVertically:D===o.height,fitsInViewportHorizontally:_==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=KM(this._overlayRef.getConfig().minHeight),a=KM(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=ZM(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!pz(this._lastScrollVisibility,i)){let r=new Fh(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let m=Math.min(i.bottom-n.y+i.top,n.y),_=this._lastBoundingBoxSize.height;o=m*2,s=n.y-m,o>_&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-_/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let m=Math.min(i.right-n.x+i.left,n.x),_=this._lastBoundingBoxSize.width;u=m*2,f=n.x-m,u>_&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-_/2)}return{top:s,left:f,bottom:a,right:h,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=ht(i.width),r.height=ht(i.height),r.top=ht(i.top)||"auto",r.bottom=ht(i.bottom)||"auto",r.left=ht(i.left)||"auto",r.right=ht(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=ht(o)),s&&(r.maxWidth=ht(s))}this._lastBoundingBoxSize=i,xs(this._boundingBox.style,r)}_resetBoundingBoxStyles(){xs(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){xs(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();xs(i,this._getExactOverlayY(e,n,u)),xs(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=ht(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=ht(s.maxWidth):o&&(i.maxWidth="")),xs(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=ht(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=ht(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:qM(n,i),isOriginOutsideView:d0(n,i),isOverlayClipped:qM(e,i),isOverlayOutsideView:d0(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Ia(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof O)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function xs(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function KM(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(hz);return!e||e==="px"?parseFloat(n):null}return t||null}function ZM(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function pz(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var XM="cdk-global-overlay-wrapper";function vo(t){return new Lh}var Lh=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(XM),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",m="",_="",D="";l?D="flex-start":u==="center"?(D="center",h?_=f:m=f):h?u==="left"||u==="end"?(D="flex-end",m=f):(u==="right"||u==="start")&&(D="flex-start",_=f):u==="left"||u==="start"?(D="flex-start",m=f):(u==="right"||u==="end")&&(D="flex-end",_=f),n.position=this._cssPosition,n.marginLeft=l?"0":m,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":_,e.justifyContent=D,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(XM),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},oT=(()=>{class t{_injector=d(Y);constructor(){}global(){return vo()}flexibleConnectedTo(e){return hd(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),pd=new b("OVERLAY_DEFAULT_CONFIG");function Or(t,n){t.get(tt).load(rT);let e=t.get(Bh),i=t.get(G),r=t.get(We),o=t.get(pn),s=t.get(Dt),a=t.get($e,null,{optional:!0})||t.get(lt).createRenderer(null,null),l=new Ar(n),c=t.get(pd,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return f0(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new Wa(new Ah(u,o,t),f,u,l,t.get(z),t.get(nT),i,t.get(Bi),t.get(iT),n?.disableAnimations??t.get(io,null,{optional:!0})==="NoopAnimations",t.get(ze),a)}var sT=(()=>{class t{scrollStrategies=d(eT);_positionBuilder=d(oT);_injector=d(Y);constructor(){}create(e){return Or(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gz=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],_z=new b("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(Y);return()=>Es(t)}}),qa=(()=>{class t{elementRef=d(O);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),aT=new b("cdk-connected-overlay-default-config"),Vh=(()=>{class t{_dir=d(Dt,{optional:!0});_injector=d(Y);_overlayRef;_templatePortal;_backdropSubscription=Se.EMPTY;_attachSubscription=Se.EMPTY;_detachSubscription=Se.EMPTY;_positionSubscription=Se.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(_z);_ngZone=d(z);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new Q;positionChange=new Q;attach=new Q;detach=new Q;overlayKeydown=new Q;overlayOutsideClick=new Q;constructor(){let e=d(Xe),i=d(ot),r=d(aT,{optional:!0}),o=d(pd,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Mr(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=gz);let e=this._overlayRef=Or(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Lt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Ar({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=hd(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof qa?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof qa?this.origin.elementRef.nativeElement:this.origin instanceof O?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Fp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",j],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",j],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",j],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",j],push:[2,"cdkConnectedOverlayPush","push",j],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",j],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",j],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Pe]})}return t})(),bi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({providers:[sT],imports:[_e,Tr,ud,ud]})}return t})();function vz(t,n){if(t&1){let e=Ze();p(0,"div",1)(1,"button",2),A("click",function(){Ae(e);let r=W();return Re(r.action())}),y(2),g()()}if(t&2){let e=W();v(2),Oe(" ",e.data.action," ")}}var yz=["label"];function bz(t,n){}var wz=Math.pow(2,31)-1,gd=class{_overlayRef;instance;containerInstance;_afterDismissed=new I;_afterOpened=new I;_onAction=new I;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,wz))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},lT=new b("MatSnackBarData"),Ya=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},Cz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),Dz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),xz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),cT=(()=>{class t{snackBarRef=d(gd);data=d(lT);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(p(0,"div",0),y(1),g(),me(2,vz,3,1,"div",1)),i&2&&(v(),Oe(" ",r.data.message,`
`),v(),he(r.hasAction?2:-1))},dependencies:[wn,Cz,Dz,xz],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),m0="_mat-snack-bar-enter",h0="_mat-snack-bar-exit",Ez=(()=>{class t extends go{_ngZone=d(z);_elementRef=d(O);_changeDetectorRef=d(Ce);_platform=d(be);_animationsDisabled=Ve();snackBarConfig=d(Ya);_document=d(G);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(Y);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new I;_onExit=new I;_onEnter=new I;_animationState="void";_live;_label;_role;_liveElementId=d(We).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===h0?this._completeExit():e===m0&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Ke(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(m0)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(m0)},200)))}exit(){return this._destroyed?q(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Ke(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(h0)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(h0),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&He(_o,7)(yz,7),i&2){let o;H(o=$())&&(r._portalOutlet=o.first),H(o=$())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&A("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&N("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[J],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(p(0,"div",1)(1,"div",2,0)(3,"div",3),se(4,bz,0,0,"ng-template",4),g(),F(5,"div"),g()()),i&2&&(v(5),te("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[_o],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),Sz=new b("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Ya}),wi=(()=>{class t{_live=d($c);_injector=d(Y);_breakpointObserver=d(_s);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(Sz);_animationsDisabled=Ve();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=cT;snackBarContainerComponent=Ez;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=w(w({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=Y.create({parent:r||this._injector,providers:[{provide:Ya,useValue:i}]}),s=new yi(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=w(w(w({},new Ya),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new gd(s,o);if(e instanceof Xe){let l=new Mr(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new yi(e,void 0,l),u=s.attachComponentPortal(c);a.instance=u.instance}return this._breakpointObserver.observe(Bc.HandsetPortrait).pipe(ue(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Ar;i.direction=e.direction;let r=vo(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Or(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return Y.create({parent:r||this._injector,providers:[{provide:gd,useValue:i},{provide:lT,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ka=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({providers:[wi],imports:[bi,Tr,Nt,cT,_e]})}return t})();var Iz=()=>[0,1,2,3,4,5];function kz(t,n){t&1&&(p(0,"mat-error"),y(1," Email is required "),g())}function Mz(t,n){t&1&&(p(0,"mat-error"),y(1," Password is required "),g())}function Tz(t,n){if(t&1){let e=Ze();p(0,"form",4),A("ngSubmit",function(){Ae(e);let r=W();return Re(r.onLoginSubmit())}),p(1,"mat-form-field",5)(2,"mat-label"),y(3,"Email Address"),g(),F(4,"input",6),se(5,kz,2,0,"mat-error",7),g(),p(6,"mat-form-field",5)(7,"mat-label"),y(8,"Password"),g(),F(9,"input",8),se(10,Mz,2,0,"mat-error",7),g(),p(11,"div",9)(12,"div",10)(13,"a",11),y(14,"Forgot Password?"),g()(),p(15,"button",12),y(16),g(),p(17,"div",13),y(18," Don't have an account? "),p(19,"a",14),y(20,"Register here"),g()()()()}if(t&2){let e,i,r=W();k("formGroup",r.loginForm),v(5),k("ngIf",(e=r.loginForm.get("email"))==null?null:e.hasError("required")),v(5),k("ngIf",(i=r.loginForm.get("password"))==null?null:i.hasError("required")),v(5),k("disabled",r.isLoading),v(),Oe(" ",r.isLoading?"Signing in...":"Sign In"," ")}}function Az(t,n){if(t&1){let e=Ze();$t(0),p(1,"input",20),A("keyup",function(r){let o=Ae(e).$implicit,s=W(2);return Re(s.onOtpInput(o,r))})("paste",function(r){Ae(e);let o=W(2);return Re(o.onOtpPaste(r))}),g(),Gt()}if(t&2){let e,i=n.$implicit,r=W(2);v(),N("invalid",((e=r.otpForm.get("otp"+i))==null?null:e.invalid)&&((e=r.otpForm.get("otp"+i))==null?null:e.touched)),k("formControlName","otp"+i)("id","otp-"+i)}}function Rz(t,n){t&1&&(p(0,"mat-error",21),y(1," Please enter the complete 6-digit OTP "),g())}function Oz(t,n){if(t&1){let e=Ze();p(0,"form",4),A("ngSubmit",function(){Ae(e);let r=W();return Re(r.onOtpSubmit())}),p(1,"div",15),y(2,"6-Digit OTP"),g(),p(3,"div",16),se(4,Az,2,4,"ng-container",17),g(),se(5,Rz,2,0,"mat-error",18),p(6,"div",9)(7,"button",12),y(8),g(),p(9,"div",13)(10,"a",19),A("click",function(){Ae(e);let r=W();return Re(r.isOtpRequired=!1)}),y(11,"Back to Login"),g()()()()}if(t&2){let e=W();k("formGroup",e.otpForm),v(4),k("ngForOf",pr(5,Iz)),v(),k("ngIf",e.otpForm.invalid&&e.otpForm.touched),v(2),k("disabled",e.isLoading),v(),Oe(" ",e.isLoading?"Verifying...":"Verify & Login"," ")}}var jh=class t{constructor(n,e,i,r,o){this.fb=n;this.authService=e;this.router=i;this.snackBar=r;this.cdr=o;this.loginForm=this.fb.group({email:["",[le.required,le.email]],password:["",le.required]}),this.otpForm=this.fb.group({otp0:["",le.required],otp1:["",le.required],otp2:["",le.required],otp3:["",le.required],otp4:["",le.required],otp5:["",le.required]})}loginForm;otpForm;isLoading=!1;isOtpRequired=!1;onLoginSubmit(){this.loginForm.invalid||(this.isLoading=!0,this.cdr.detectChanges(),this.authService.login(this.loginForm.value).subscribe({next:n=>{setTimeout(()=>{this.isLoading=!1,n.isOtpRequired?(this.isOtpRequired=!0,this.snackBar.open("Please check your email for the OTP code.","Close",{duration:5e3})):this.router.navigate(["/dashboard"]),this.cdr.detectChanges()})},error:n=>{setTimeout(()=>{this.isLoading=!1;let e=n.error?.message||"Login failed";this.snackBar.open(e,"Close",{duration:4e3}),this.cdr.detectChanges()})}}))}onOtpSubmit(){if(this.otpForm.invalid)return;this.isLoading=!0,this.cdr.detectChanges();let n=`${this.otpForm.value.otp0}${this.otpForm.value.otp1}${this.otpForm.value.otp2}${this.otpForm.value.otp3}${this.otpForm.value.otp4}${this.otpForm.value.otp5}`,e={email:this.loginForm.value.email,otp:n};this.authService.verifyOtp(e).subscribe({next:()=>{this.snackBar.open("Login successful!","Close",{duration:3e3}),this.router.navigate(["/dashboard"])},error:i=>{setTimeout(()=>{this.isLoading=!1;let r=i.error?.message||"Invalid OTP";this.snackBar.open(r,"Close",{duration:4e3}),this.cdr.detectChanges()})}})}onOtpInput(n,e){let i=e.target;if(i.value&&n<5){let r=document.getElementById(`otp-${n+1}`);r&&r.focus()}if(e.key==="Backspace"&&!i.value&&n>0){let r=document.getElementById(`otp-${n-1}`);r&&r.focus()}}onOtpPaste(n){n.preventDefault();let e=n.clipboardData?.getData("text");if(!e)return;let i=e.replace(/\D/g,"").substring(0,6);if(!i)return;let r={};for(let a=0;a<i.length;a++)r[`otp${a}`]=i[a];this.otpForm.patchValue(r);let o=i.length<6?i.length:5,s=document.getElementById(`otp-${o}`);s&&s.focus()}static \u0275fac=function(e){return new(e||t)(R(Qi),R(bn),R(mt),R(wi),R(Ce))};static \u0275cmp=T({type:t,selectors:[["app-login"]],decls:9,vars:3,consts:[[1,"auth-container"],[1,"auth-card"],[1,"auth-header"],[3,"formGroup","ngSubmit",4,"ngIf"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"form-field-full"],["matInput","","formControlName","email","type","email","placeholder","john@example.com"],[4,"ngIf"],["matInput","","formControlName","password","type","password"],[1,"actions"],[1,"forgot-password-link",2,"text-align","right","margin-bottom","16px"],["routerLink","/forgot-password",2,"font-size","14px","color","var(--primary-color)","text-decoration","none"],["mat-flat-button","","type","submit",1,"primary-button",3,"disabled"],[1,"links"],["routerLink","/register"],[2,"text-align","left","font-size","14px","margin-bottom","8px"],[1,"otp-boxes-container",2,"display","flex","gap","8px","justify-content","center","margin-bottom","16px"],[4,"ngFor","ngForOf"],["style","text-align: center; margin-bottom: 16px; display: block; font-size: 12px; color: #f44336;",4,"ngIf"],[3,"click"],["type","text","maxlength","1",1,"otp-input",3,"keyup","paste","formControlName","id"],[2,"text-align","center","margin-bottom","16px","display","block","font-size","12px","color","#f44336"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"h1"),y(4,"Welcome Back"),g(),p(5,"p"),y(6),g()(),se(7,Tz,21,5,"form",3)(8,Oz,12,6,"form",3),g()()),e&2&&(v(6),Pt(i.isOtpRequired?"Enter the OTP sent to your email.":"Sign in to continue to your dashboard."),v(),k("ngIf",!i.isOtpRequired),v(),k("ngIf",i.isOtpRequired))},dependencies:[Rt,sa,ui,Yi,qi,Wn,Gi,Wi,rd,dn,_i,Un,Zi,Ki,Bt,Kt,un,Sr,Qn,qn,Nt,wn,Ka],styles:[".otp-input[_ngcontent-%COMP%]{width:40px;height:48px;text-align:center;font-size:20px;border:1px solid #ccc;border-radius:4px}@media(max-width:400px){.otp-boxes-container[_ngcontent-%COMP%]{gap:4px}.otp-input[_ngcontent-%COMP%]{width:32px;height:40px;font-size:16px}}"]})};function Nz(t,n){t&1&&(p(0,"mat-error"),y(1," Full Name is required "),g())}function Fz(t,n){t&1&&(p(0,"mat-error"),y(1," Company Name is required "),g())}function Pz(t,n){t&1&&(p(0,"mat-error"),y(1," Email is required "),g())}function Lz(t,n){t&1&&(p(0,"mat-error"),y(1," Please enter a valid email address "),g())}function Bz(t,n){t&1&&(p(0,"mat-error"),y(1," Password is required "),g())}function Vz(t,n){t&1&&(p(0,"mat-error"),y(1," Password must be at least 6 characters "),g())}function jz(t,n){t&1&&(p(0,"mat-error"),y(1," Confirm Password is required "),g())}function zz(t,n){t&1&&(p(0,"mat-error"),y(1," Passwords do not match "),g())}function Uz(t){if(!t.parent)return null;let n=t.parent.get("password")?.value,e=t.value;return n&&e&&n!==e?{passwordMismatch:!0}:null}var zh=class t{constructor(n,e,i,r,o){this.fb=n;this.authService=e;this.router=i;this.snackBar=r;this.cdr=o;this.registerForm=this.fb.group({fullName:["",le.required],companyName:["",le.required],email:["",[le.required,le.email]],phoneNumber:[""],password:["",[le.required,le.minLength(6)]],confirmPassword:["",[le.required,Uz]]})}registerForm;isLoading=X(!1);ngOnInit(){this.registerForm.get("password")?.valueChanges.subscribe(()=>{this.registerForm.get("confirmPassword")?.updateValueAndValidity()})}onSubmit(){this.registerForm.invalid||(this.isLoading.set(!0),this.authService.register(this.registerForm.value).subscribe({next:()=>{this.isLoading.set(!1),this.snackBar.open("Registration successful! Please login.","Close",{duration:3e3}),this.router.navigate(["/login"])},error:n=>{this.isLoading.set(!1);let e=n.error?.message||n.error?.[0]?.description||"Registration failed";this.snackBar.open(e,"Close",{duration:4e3})}}))}static \u0275fac=function(e){return new(e||t)(R(Qi),R(bn),R(mt),R(wi),R(Ce))};static \u0275cmp=T({type:t,selectors:[["app-register"]],decls:50,vars:11,consts:[[1,"auth-container"],[1,"auth-card"],[1,"auth-header"],[3,"ngSubmit","formGroup"],[1,"form-row"],["appearance","outline",1,"form-field-full"],["matInput","","formControlName","fullName","placeholder","John Doe"],[4,"ngIf"],["matInput","","formControlName","companyName","placeholder","Acme Corp"],["matInput","","formControlName","email","type","email","placeholder","john@example.com"],["matInput","","formControlName","phoneNumber","type","tel","placeholder","+1234567890"],["matInput","","formControlName","password","type","password"],["matInput","","formControlName","confirmPassword","type","password"],[1,"actions"],["mat-flat-button","","type","submit",1,"primary-button",3,"disabled"],[1,"links"],["routerLink","/login"]],template:function(e,i){if(e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"h1"),y(4,"Create an Account"),g(),p(5,"p"),y(6,"Join us to access the dashboard."),g()(),p(7,"form",3),A("ngSubmit",function(){return i.onSubmit()}),p(8,"div",4)(9,"mat-form-field",5)(10,"mat-label"),y(11,"Full Name"),g(),F(12,"input",6),se(13,Nz,2,0,"mat-error",7),g(),p(14,"mat-form-field",5)(15,"mat-label"),y(16,"Company Name (Business)"),g(),F(17,"input",8),se(18,Fz,2,0,"mat-error",7),g()(),p(19,"div",4)(20,"mat-form-field",5)(21,"mat-label"),y(22,"Email Address"),g(),F(23,"input",9),se(24,Pz,2,0,"mat-error",7)(25,Lz,2,0,"mat-error",7),g(),p(26,"mat-form-field",5)(27,"mat-label"),y(28,"Phone Number"),g(),F(29,"input",10),g()(),p(30,"div",4)(31,"mat-form-field",5)(32,"mat-label"),y(33,"Password"),g(),F(34,"input",11),se(35,Bz,2,0,"mat-error",7)(36,Vz,2,0,"mat-error",7),g(),p(37,"mat-form-field",5)(38,"mat-label"),y(39,"Confirm Password"),g(),F(40,"input",12),se(41,jz,2,0,"mat-error",7)(42,zz,2,0,"mat-error",7),g()(),p(43,"div",13)(44,"button",14),y(45),g(),p(46,"div",15),y(47," Already have an account? "),p(48,"a",16),y(49,"Log in here"),g()()()()()()),e&2){let r,o,s,a,l,c,u,f;v(7),k("formGroup",i.registerForm),v(6),k("ngIf",(r=i.registerForm.get("fullName"))==null?null:r.hasError("required")),v(5),k("ngIf",(o=i.registerForm.get("companyName"))==null?null:o.hasError("required")),v(6),k("ngIf",(s=i.registerForm.get("email"))==null?null:s.hasError("required")),v(),k("ngIf",(a=i.registerForm.get("email"))==null?null:a.hasError("email")),v(10),k("ngIf",(l=i.registerForm.get("password"))==null?null:l.hasError("required")),v(),k("ngIf",(c=i.registerForm.get("password"))==null?null:c.hasError("minlength")),v(5),k("ngIf",(u=i.registerForm.get("confirmPassword"))==null?null:u.hasError("required")),v(),k("ngIf",(f=i.registerForm.get("confirmPassword"))==null?null:f.hasError("passwordMismatch")),v(2),k("disabled",i.isLoading()),v(),Oe(" ",i.isLoading()?"Creating account...":"Register"," ")}},dependencies:[Rt,ui,Yi,qi,Wn,Gi,Wi,dn,_i,Un,Zi,Ki,Bt,Kt,un,Sr,Qn,qn,Nt,wn,Ka],styles:[".register-card[_ngcontent-%COMP%]{max-width:1560px!important}.form-row[_ngcontent-%COMP%]{display:flex;gap:16px}.form-row[_ngcontent-%COMP%]   .form-field-full[_ngcontent-%COMP%]{flex:1}@media(max-width:480px){.form-row[_ngcontent-%COMP%]{flex-direction:column;gap:0}}"]})};var Hz=()=>[0,1,2,3,4,5];function $z(t,n){t&1&&(p(0,"p"),y(1,"Enter your email to receive a password reset OTP."),g())}function Gz(t,n){t&1&&(p(0,"p"),y(1,"Enter the 6-digit OTP sent to your email."),g())}function Wz(t,n){t&1&&(p(0,"p"),y(1,"Enter your new password."),g())}function qz(t,n){t&1&&(p(0,"mat-error"),y(1," Email is required "),g())}function Qz(t,n){if(t&1){let e=Ze();p(0,"form",5),A("ngSubmit",function(){Ae(e);let r=W();return Re(r.onRequestOtp())}),p(1,"mat-form-field",6)(2,"mat-label"),y(3,"Email Address"),g(),F(4,"input",7),se(5,qz,2,0,"mat-error",3),g(),p(6,"div",8)(7,"button",9),y(8),g(),p(9,"div",10),y(10," Remember your password? "),p(11,"a",11),y(12,"Log in here"),g()()()()}if(t&2){let e,i=W();k("formGroup",i.emailForm),v(5),k("ngIf",(e=i.emailForm.get("email"))==null?null:e.hasError("required")),v(2),k("disabled",i.emailForm.invalid||i.isLoading()),v(),Oe(" ",i.isLoading()?"Sending...":"Send OTP"," ")}}function Yz(t,n){if(t&1){let e=Ze();$t(0),p(1,"input",18),A("keyup",function(r){let o=Ae(e).$implicit,s=W(2);return Re(s.onOtpInput(o,r))})("paste",function(r){Ae(e);let o=W(2);return Re(o.onOtpPaste(r))}),g(),Gt()}if(t&2){let e,i=n.$implicit,r=W(2);v(),N("invalid",((e=r.otpForm.get("otp"+i))==null?null:e.invalid)&&((e=r.otpForm.get("otp"+i))==null?null:e.touched)),k("formControlName","otp"+i)("id","reset-otp-"+i)}}function Kz(t,n){t&1&&(p(0,"mat-error",19),y(1," Please enter the complete 6-digit OTP "),g())}function Zz(t,n){if(t&1){let e=Ze();p(0,"form",5),A("ngSubmit",function(){Ae(e);let r=W();return Re(r.onVerifyOtp())}),p(1,"div",12),y(2,"6-Digit OTP"),g(),p(3,"div",13),se(4,Yz,2,4,"ng-container",14),g(),se(5,Kz,2,0,"mat-error",15),p(6,"div",8)(7,"button",9),y(8),g(),p(9,"div",10)(10,"a",16),A("click",function(){Ae(e);let r=W();return Re(r.onRequestOtp())}),y(11,"Resend OTP"),g(),p(12,"a",17),A("click",function(){Ae(e);let r=W();return Re(r.step="email")}),y(13,"Back to Email"),g()()()()}if(t&2){let e=W();k("formGroup",e.otpForm),v(4),k("ngForOf",pr(5,Hz)),v(),k("ngIf",e.otpForm.invalid&&e.otpForm.touched),v(2),k("disabled",e.otpForm.invalid||e.isLoading()),v(),Oe(" ",e.isLoading()?"Verifying...":"Verify OTP"," ")}}function Xz(t,n){t&1&&(p(0,"mat-error"),y(1," New password is required "),g())}function Jz(t,n){t&1&&(p(0,"mat-error"),y(1," Password must be at least 6 characters "),g())}function eU(t,n){t&1&&(p(0,"mat-error"),y(1," Please confirm your password "),g())}function tU(t,n){t&1&&(p(0,"mat-error"),y(1," Passwords do not match "),g())}function nU(t,n){if(t&1){let e=Ze();p(0,"form",5),A("ngSubmit",function(){Ae(e);let r=W();return Re(r.onResetSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),y(3,"New Password"),g(),F(4,"input",20),se(5,Xz,2,0,"mat-error",3)(6,Jz,2,0,"mat-error",3),g(),p(7,"mat-form-field",6)(8,"mat-label"),y(9,"Confirm New Password"),g(),F(10,"input",21),se(11,eU,2,0,"mat-error",3)(12,tU,2,0,"mat-error",3),g(),p(13,"div",8)(14,"button",9),y(15),g(),p(16,"div",10)(17,"a",17),A("click",function(){Ae(e);let r=W();return Re(r.step="otp")}),y(18,"Back to OTP"),g()()()()}if(t&2){let e,i,r,o,s=W();k("formGroup",s.passwordForm),v(5),k("ngIf",(e=s.passwordForm.get("newPassword"))==null?null:e.hasError("required")),v(),k("ngIf",(i=s.passwordForm.get("newPassword"))==null?null:i.hasError("minlength")),v(5),k("ngIf",(r=s.passwordForm.get("confirmPassword"))==null?null:r.hasError("required")),v(),k("ngIf",(o=s.passwordForm.get("confirmPassword"))==null?null:o.hasError("passwordMismatch")),v(2),k("disabled",s.passwordForm.invalid||s.isLoading()),v(),Oe(" ",s.isLoading()?"Resetting...":"Reset Password"," ")}}function iU(t){if(!t.parent)return null;let n=t.parent.get("newPassword")?.value,e=t.value;return n&&e&&n!==e?{passwordMismatch:!0}:null}var Uh=class t{constructor(n,e,i,r){this.fb=n;this.authService=e;this.router=i;this.snackBar=r;this.emailForm=this.fb.group({email:["",[le.required,le.email]]}),this.otpForm=this.fb.group({otp0:["",le.required],otp1:["",le.required],otp2:["",le.required],otp3:["",le.required],otp4:["",le.required],otp5:["",le.required]}),this.passwordForm=this.fb.group({newPassword:["",[le.required,le.minLength(6)]],confirmPassword:["",[le.required,iU]]})}step="email";emailForm;otpForm;passwordForm;isLoading=X(!1);ngOnInit(){this.passwordForm.get("newPassword")?.valueChanges.subscribe(()=>{this.passwordForm.get("confirmPassword")?.updateValueAndValidity()})}onRequestOtp(){this.emailForm.invalid||(this.isLoading.set(!0),this.authService.forgotPassword({email:this.emailForm.value.email}).subscribe({next:n=>{this.isLoading.set(!1),this.step="otp",this.snackBar.open(n.message||"OTP sent successfully","Close",{duration:5e3})},error:n=>{this.isLoading.set(!1),this.snackBar.open("Failed to request OTP","Close",{duration:4e3})}}))}onVerifyOtp(){if(this.otpForm.invalid)return;this.isLoading.set(!0);let n=`${this.otpForm.value.otp0}${this.otpForm.value.otp1}${this.otpForm.value.otp2}${this.otpForm.value.otp3}${this.otpForm.value.otp4}${this.otpForm.value.otp5}`;this.authService.verifyResetOtp({email:this.emailForm.value.email,otp:n}).subscribe({next:()=>{this.isLoading.set(!1),this.step="password",this.snackBar.open("OTP verified! Please enter your new password.","Close",{duration:4e3})},error:e=>{this.isLoading.set(!1);let i=e.error?.message||"Invalid OTP";this.snackBar.open(i,"Close",{duration:4e3})}})}onResetSubmit(){if(this.passwordForm.invalid)return;this.isLoading.set(!0);let n=`${this.otpForm.value.otp0}${this.otpForm.value.otp1}${this.otpForm.value.otp2}${this.otpForm.value.otp3}${this.otpForm.value.otp4}${this.otpForm.value.otp5}`,e={email:this.emailForm.value.email,otp:n,newPassword:this.passwordForm.value.newPassword};this.authService.resetPassword(e).subscribe({next:()=>{this.isLoading.set(!1),this.snackBar.open("Password reset successfully! Please login.","Close",{duration:4e3}),this.router.navigate(["/login"])},error:i=>{this.isLoading.set(!1);let r=i.error?.message||"Password reset failed";this.snackBar.open(r,"Close",{duration:4e3})}})}onOtpInput(n,e){let i=e.target;if(i.value&&n<5){let r=document.getElementById(`reset-otp-${n+1}`);r&&r.focus()}if(e.key==="Backspace"&&!i.value&&n>0){let r=document.getElementById(`reset-otp-${n-1}`);r&&r.focus()}}onOtpPaste(n){n.preventDefault();let e=n.clipboardData?.getData("text");if(!e)return;let i=e.replace(/\D/g,"").substring(0,6);if(!i)return;let r={};for(let a=0;a<i.length;a++)r[`otp${a}`]=i[a];this.otpForm.patchValue(r);let o=i.length<6?i.length:5,s=document.getElementById(`reset-otp-${o}`);s&&s.focus()}static \u0275fac=function(e){return new(e||t)(R(Qi),R(bn),R(mt),R(wi))};static \u0275cmp=T({type:t,selectors:[["app-forgot-password"]],decls:11,vars:6,consts:[[1,"auth-container"],[1,"auth-card"],[1,"auth-header"],[4,"ngIf"],[3,"formGroup","ngSubmit",4,"ngIf"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"form-field-full"],["matInput","","formControlName","email","type","email","placeholder","john@example.com"],[1,"actions"],["mat-flat-button","","type","submit",1,"primary-button",3,"disabled"],[1,"links"],["routerLink","/login"],[2,"text-align","left","font-size","14px","margin-bottom","8px"],[1,"otp-boxes-container",2,"display","flex","gap","8px","justify-content","center","margin-bottom","16px"],[4,"ngFor","ngForOf"],["style","text-align: center; margin-bottom: 16px; display: block; font-size: 12px;",4,"ngIf"],[2,"cursor","pointer","margin-right","16px",3,"click"],[2,"cursor","pointer",3,"click"],["type","text","maxlength","1",1,"otp-input",3,"keyup","paste","formControlName","id"],[2,"text-align","center","margin-bottom","16px","display","block","font-size","12px"],["matInput","","formControlName","newPassword","type","password"],["matInput","","formControlName","confirmPassword","type","password"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"h1"),y(4,"Forgot Password"),g(),se(5,$z,2,0,"p",3)(6,Gz,2,0,"p",3)(7,Wz,2,0,"p",3),g(),se(8,Qz,13,4,"form",4)(9,Zz,14,6,"form",4)(10,nU,19,7,"form",4),g()()),e&2&&(v(5),k("ngIf",i.step==="email"),v(),k("ngIf",i.step==="otp"),v(),k("ngIf",i.step==="password"),v(),k("ngIf",i.step==="email"),v(),k("ngIf",i.step==="otp"),v(),k("ngIf",i.step==="password"))},dependencies:[Rt,sa,ui,Yi,qi,Wn,Gi,Wi,rd,dn,_i,Un,Zi,Ki,Bt,Kt,un,Sr,Qn,qn,Nt,wn,Ka],styles:[".otp-input[_ngcontent-%COMP%]{width:40px;height:48px;text-align:center;font-size:20px;border:1px solid #ccc;border-radius:4px}@media(max-width:400px){.otp-boxes-container[_ngcontent-%COMP%]{gap:4px}.otp-input[_ngcontent-%COMP%]{width:32px;height:40px;font-size:16px}}"]})};var Gh=["*"],rU=["content"],oU=[[["mat-drawer"]],[["mat-drawer-content"]],"*"],sU=["mat-drawer","mat-drawer-content","*"];function aU(t,n){if(t&1){let e=Ze();p(0,"div",1),A("click",function(){Ae(e);let r=W();return Re(r._onBackdropClicked())}),g()}if(t&2){let e=W();N("mat-drawer-shown",e._isShowingBackdrop())}}function lU(t,n){t&1&&(p(0,"mat-drawer-content"),U(1,2),g())}var cU=[[["mat-sidenav"]],[["mat-sidenav-content"]],"*"],dU=["mat-sidenav","mat-sidenav-content","*"];function uU(t,n){if(t&1){let e=Ze();p(0,"div",1),A("click",function(){Ae(e);let r=W();return Re(r._onBackdropClicked())}),g()}if(t&2){let e=W();N("mat-drawer-shown",e._isShowingBackdrop())}}function fU(t,n){t&1&&(p(0,"mat-sidenav-content"),U(1,2),g())}var mU=`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`;var hU=new b("MAT_DRAWER_DEFAULT_AUTOSIZE",{providedIn:"root",factory:()=>!1}),_0=new b("MAT_DRAWER_CONTAINER"),Hh=(()=>{class t extends kr{_platform=d(be);_changeDetectorRef=d(Ce);_container=d(g0);constructor(){let e=d(O),i=d(Ir),r=d(z);super(e,i,r)}ngAfterContentInit(){this._container._contentMarginChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()})}_shouldBeHidden(){if(this._platform.isBrowser)return!1;let{start:e,end:i}=this._container;return e!=null&&e.mode!=="over"&&e.opened||i!=null&&i.mode!=="over"&&i.opened}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer-content"]],hostAttrs:[1,"mat-drawer-content"],hostVars:6,hostBindings:function(i,r){i&2&&(hr("margin-left",r._container._contentMargins.left,"px")("margin-right",r._container._contentMargins.right,"px"),N("mat-drawer-content-hidden",r._shouldBeHidden()))},features:[ge([{provide:kr,useExisting:t}]),J],ngContentSelectors:Gh,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},encapsulation:2,changeDetection:0})}return t})(),p0=(()=>{class t{_elementRef=d(O);_focusTrapFactory=d(Hc);_focusMonitor=d(Mn);_platform=d(be);_ngZone=d(z);_renderer=d($e);_interactivityChecker=d(Oa);_doc=d(G);_container=d(_0,{optional:!0});_focusTrap=null;_elementFocusedBeforeDrawerWasOpened=null;_eventCleanups;_isAttached=!1;_anchor=null;get position(){return this._position}set position(e){e=e==="end"?"end":"start",e!==this._position&&(this._isAttached&&this._updatePositionInParent(e),this._position=e,this.onPositionChanged.emit())}_position="start";get mode(){return this._mode}set mode(e){this._mode=e,this._updateFocusTrapState(),this._modeChanged.next()}_mode="over";get disableClose(){return this._disableClose}set disableClose(e){this._disableClose=ct(e)}_disableClose=!1;get autoFocus(){let e=this._autoFocus;return e??(this.mode==="side"?"dialog":"first-tabbable")}set autoFocus(e){(e==="true"||e==="false"||e==null)&&(e=ct(e)),this._autoFocus=e}_autoFocus;get opened(){return this._opened()}set opened(e){this.toggle(ct(e))}_opened=X(!1);_openedVia=null;_animationStarted=new I;_animationEnd=new I;openedChange=new Q(!0);_openedStream=this.openedChange.pipe(Ie(e=>e),ee(()=>{}));openedStart=this._animationStarted.pipe(Ie(()=>this.opened),ou(void 0));_closedStream=this.openedChange.pipe(Ie(e=>!e),ee(()=>{}));closedStart=this._animationStarted.pipe(Ie(()=>!this.opened),ou(void 0));_destroyed=new I;onPositionChanged=new Q;_content;_modeChanged=new I;_injector=d(Y);_changeDetectorRef=d(Ce);constructor(){this.openedChange.pipe(ue(this._destroyed)).subscribe(e=>{e?(this._elementFocusedBeforeDrawerWasOpened=this._doc.activeElement,this._takeFocus()):this._isFocusWithinDrawer()&&this._restoreFocus(this._openedVia||"program")}),this._eventCleanups=this._ngZone.runOutsideAngular(()=>{let e=this._renderer,i=this._elementRef.nativeElement;return[e.listen(i,"keydown",r=>{r.keyCode===27&&!this.disableClose&&!Ot(r)&&this._ngZone.run(()=>{this.close(),r.stopPropagation(),r.preventDefault()})}),e.listen(i,"transitionend",this._handleTransitionEvent),e.listen(i,"transitioncancel",this._handleTransitionEvent)]}),this._animationEnd.subscribe(()=>{this.openedChange.emit(this.opened)})}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_takeFocus(){if(!this._focusTrap)return;let e=this._elementRef.nativeElement;switch(this.autoFocus){case!1:case"dialog":return;case!0:case"first-tabbable":Ke(()=>{!this._focusTrap.focusInitialElement()&&typeof e.focus=="function"&&e.focus()},{injector:this._injector});break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]');break;default:this._focusByCssSelector(this.autoFocus);break}}_restoreFocus(e){this.autoFocus!=="dialog"&&(this._elementFocusedBeforeDrawerWasOpened?this._focusMonitor.focusVia(this._elementFocusedBeforeDrawerWasOpened,e):this._elementRef.nativeElement.blur(),this._elementFocusedBeforeDrawerWasOpened=null)}_isFocusWithinDrawer(){let e=this._doc.activeElement;return!!e&&this._elementRef.nativeElement.contains(e)}ngAfterViewInit(){this._isAttached=!0,this._position==="end"&&this._updatePositionInParent("end"),this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._updateFocusTrapState())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._focusTrap?.destroy(),this._anchor?.remove(),this._anchor=null,this._animationStarted.complete(),this._animationEnd.complete(),this._modeChanged.complete(),this._destroyed.next(),this._destroyed.complete()}open(e){return this.toggle(!0,e)}close(){return this.toggle(!1)}_closeViaBackdropClick(){return this._setOpen(!1,!0,"mouse")}toggle(e=!this.opened,i){e&&i&&(this._openedVia=i);let r=this._setOpen(e,!e&&this._isFocusWithinDrawer(),this._openedVia||"program");return e||(this._openedVia=null),r}_setOpen(e,i,r){return e===this.opened?Promise.resolve(e?"open":"close"):(this._opened.set(e),this._container?._transitionsEnabled?(this._setIsAnimating(!0),setTimeout(()=>this._animationStarted.next())):setTimeout(()=>{this._animationStarted.next(),this._animationEnd.next()}),this._elementRef.nativeElement.classList.toggle("mat-drawer-opened",e),!e&&i&&this._restoreFocus(r),this._changeDetectorRef.markForCheck(),this._updateFocusTrapState(),new Promise(o=>{this.openedChange.pipe(rt(1)).subscribe(s=>o(s?"open":"close"))}))}_setIsAnimating(e){this._elementRef.nativeElement.classList.toggle("mat-drawer-animating",e)}_getWidth(){return this._elementRef.nativeElement.offsetWidth||0}_updateFocusTrapState(){this._focusTrap&&(this._focusTrap.enabled=this.opened&&!!this._container?._isShowingBackdrop())}_updatePositionInParent(e){if(!this._platform.isBrowser)return;let i=this._elementRef.nativeElement,r=i.parentNode;e==="end"?(this._anchor||(this._anchor=this._doc.createComment("mat-drawer-anchor"),r.insertBefore(this._anchor,i)),r.appendChild(i)):this._anchor&&this._anchor.parentNode.insertBefore(i,this._anchor)}_handleTransitionEvent=e=>{let i=this._elementRef.nativeElement;e.target===i&&this._ngZone.run(()=>{e.type==="transitionend"&&this._setIsAnimating(!1),this._animationEnd.next(e)})};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer"]],viewQuery:function(i,r){if(i&1&&He(rU,5),i&2){let o;H(o=$())&&(r._content=o.first)}},hostAttrs:[1,"mat-drawer"],hostVars:12,hostBindings:function(i,r){i&2&&(te("align",null)("tabIndex",r.mode!=="side"?"-1":null),hr("visibility",!r._container&&!r.opened?"hidden":null),N("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side"))},inputs:{position:"position",mode:"mode",disableClose:"disableClose",autoFocus:"autoFocus",opened:"opened"},outputs:{openedChange:"openedChange",_openedStream:"opened",openedStart:"openedStart",_closedStream:"closed",closedStart:"closedStart",onPositionChanged:"positionChanged"},exportAs:["matDrawer"],ngContentSelectors:Gh,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(xe(),p(0,"div",1,0),U(2),g())},dependencies:[kr],encapsulation:2,changeDetection:0})}return t})(),g0=(()=>{class t{_dir=d(Dt,{optional:!0});_element=d(O);_ngZone=d(z);_changeDetectorRef=d(Ce);_animationDisabled=Ve();_transitionsEnabled=!1;_allDrawers;_drawers=new cr;_content;_userContent;get start(){return this._start}get end(){return this._end}get autosize(){return this._autosize}set autosize(e){this._autosize=ct(e)}_autosize=d(hU);get hasBackdrop(){return this._drawerHasBackdrop(this._start)||this._drawerHasBackdrop(this._end)}set hasBackdrop(e){this._backdropOverride=e==null?null:ct(e)}_backdropOverride=null;backdropClick=new Q;_start=null;_end=null;_left=null;_right=null;_destroyed=new I;_doCheckSubject=new I;_contentMargins={left:null,right:null};_contentMarginChanges=new I;get scrollable(){return this._userContent||this._content}_injector=d(Y);constructor(){let e=d(be),i=d(Yn);this._dir?.change.pipe(ue(this._destroyed)).subscribe(()=>{this._validateDrawers(),this.updateContentMargins()}),i.change().pipe(ue(this._destroyed)).subscribe(()=>this.updateContentMargins()),!this._animationDisabled&&e.isBrowser&&this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._element.nativeElement.classList.add("mat-drawer-transition"),this._transitionsEnabled=!0},200)})}ngAfterContentInit(){this._allDrawers.changes.pipe(St(this._allDrawers),ue(this._destroyed)).subscribe(e=>{this._drawers.reset(e.filter(i=>!i._container||i._container===this)),this._drawers.notifyOnChanges()}),this._drawers.changes.pipe(St(null)).subscribe(()=>{this._validateDrawers(),this._drawers.forEach(e=>{this._watchDrawerToggle(e),this._watchDrawerPosition(e),this._watchDrawerMode(e)}),(!this._drawers.length||this._isDrawerOpen(this._start)||this._isDrawerOpen(this._end))&&this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),this._ngZone.runOutsideAngular(()=>{this._doCheckSubject.pipe(tr(10),ue(this._destroyed)).subscribe(()=>this.updateContentMargins())})}ngOnDestroy(){this._contentMarginChanges.complete(),this._doCheckSubject.complete(),this._drawers.destroy(),this._destroyed.next(),this._destroyed.complete()}open(){this._drawers.forEach(e=>e.open())}close(){this._drawers.forEach(e=>e.close())}updateContentMargins(){let e=0,i=0;if(this._left&&this._left.opened){if(this._left.mode=="side")e+=this._left._getWidth();else if(this._left.mode=="push"){let r=this._left._getWidth();e+=r,i-=r}}if(this._right&&this._right.opened){if(this._right.mode=="side")i+=this._right._getWidth();else if(this._right.mode=="push"){let r=this._right._getWidth();i+=r,e-=r}}e=e||null,i=i||null,(e!==this._contentMargins.left||i!==this._contentMargins.right)&&(this._contentMargins={left:e,right:i},this._ngZone.run(()=>this._contentMarginChanges.next(this._contentMargins)))}ngDoCheck(){this._autosize&&this._isPushed()&&this._ngZone.runOutsideAngular(()=>this._doCheckSubject.next())}_watchDrawerToggle(e){e._animationStarted.pipe(ue(this._drawers.changes)).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()}),e.mode!=="side"&&e.openedChange.pipe(ue(this._drawers.changes)).subscribe(()=>this._setContainerClass(e.opened))}_watchDrawerPosition(e){e.onPositionChanged.pipe(ue(this._drawers.changes)).subscribe(()=>{Ke({read:()=>this._validateDrawers()},{injector:this._injector})})}_watchDrawerMode(e){e._modeChanged.pipe(ue(Et(this._drawers.changes,this._destroyed))).subscribe(()=>{this.updateContentMargins(),this._changeDetectorRef.markForCheck()})}_setContainerClass(e){let i=this._element.nativeElement.classList,r="mat-drawer-container-has-open";e?i.add(r):i.remove(r)}_validateDrawers(){this._start=this._end=null,this._drawers.forEach(e=>{e.position=="end"?(this._end!=null,this._end=e):(this._start!=null,this._start=e)}),this._right=this._left=null,this._dir&&this._dir.value==="rtl"?(this._left=this._end,this._right=this._start):(this._left=this._start,this._right=this._end)}_isPushed(){return this._isDrawerOpen(this._start)&&this._start.mode!="over"||this._isDrawerOpen(this._end)&&this._end.mode!="over"}_onBackdropClicked(){this.backdropClick.emit(),this._closeModalDrawersViaBackdrop()}_closeModalDrawersViaBackdrop(){[this._start,this._end].filter(e=>e&&!e.disableClose&&this._drawerHasBackdrop(e)).forEach(e=>e._closeViaBackdropClick())}_isShowingBackdrop(){return this._isDrawerOpen(this._start)&&this._drawerHasBackdrop(this._start)||this._isDrawerOpen(this._end)&&this._drawerHasBackdrop(this._end)}_isDrawerOpen(e){return e!=null&&e.opened}_drawerHasBackdrop(e){return this._backdropOverride==null?!!e&&e.mode!=="side":this._backdropOverride}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-drawer-container"]],contentQueries:function(i,r,o){if(i&1&&wt(o,Hh,5)(o,p0,5),i&2){let s;H(s=$())&&(r._content=s.first),H(s=$())&&(r._allDrawers=s)}},viewQuery:function(i,r){if(i&1&&He(Hh,5),i&2){let o;H(o=$())&&(r._userContent=o.first)}},hostAttrs:[1,"mat-drawer-container"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-drawer-container-explicit-backdrop",r._backdropOverride)},inputs:{autosize:"autosize",hasBackdrop:"hasBackdrop"},outputs:{backdropClick:"backdropClick"},exportAs:["matDrawerContainer"],features:[ge([{provide:_0,useExisting:t}])],ngContentSelectors:sU,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(xe(oU),me(0,aU,1,2,"div",0),U(1),U(2,1),me(3,lU,2,0,"mat-drawer-content")),i&2&&(he(r.hasBackdrop?0:-1),v(3),he(r._content?-1:3))},dependencies:[Hh],styles:[`.mat-drawer-container {
  position: relative;
  z-index: 1;
  color: var(--mat-sidenav-content-text-color, var(--mat-sys-on-background));
  background-color: var(--mat-sidenav-content-background-color, var(--mat-sys-background));
  box-sizing: border-box;
  display: block;
  overflow: hidden;
}
.mat-drawer-container[fullscreen] {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-drawer-container[fullscreen].mat-drawer-container-has-open {
  overflow: hidden;
}
.mat-drawer-container.mat-drawer-container-explicit-backdrop .mat-drawer-side {
  z-index: 3;
}
.mat-drawer-container.ng-animate-disabled .mat-drawer-backdrop,
.mat-drawer-container.ng-animate-disabled .mat-drawer-content, .ng-animate-disabled .mat-drawer-container .mat-drawer-backdrop,
.ng-animate-disabled .mat-drawer-container .mat-drawer-content {
  transition: none;
}

.mat-drawer-backdrop {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  z-index: 3;
  visibility: hidden;
}
.mat-drawer-backdrop.mat-drawer-shown {
  visibility: visible;
  background-color: var(--mat-sidenav-scrim-color, color-mix(in srgb, var(--mat-sys-neutral-variant20) 40%, transparent));
}
.mat-drawer-transition .mat-drawer-backdrop {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: background-color, visibility;
}
@media (forced-colors: active) {
  .mat-drawer-backdrop {
    opacity: 0.5;
  }
}

.mat-drawer-content {
  position: relative;
  z-index: 1;
  display: block;
  height: 100%;
  overflow: auto;
}
.mat-drawer-content.mat-drawer-content-hidden {
  opacity: 0;
}
.mat-drawer-transition .mat-drawer-content {
  transition-duration: 400ms;
  transition-timing-function: cubic-bezier(0.25, 0.8, 0.25, 1);
  transition-property: transform, margin-left, margin-right;
}

.mat-drawer {
  position: relative;
  z-index: 4;
  color: var(--mat-sidenav-container-text-color, var(--mat-sys-on-surface-variant));
  box-shadow: var(--mat-sidenav-container-elevation-shadow, none);
  background-color: var(--mat-sidenav-container-background-color, var(--mat-sys-surface));
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  width: var(--mat-sidenav-container-width, 360px);
  display: block;
  position: absolute;
  top: 0;
  bottom: 0;
  z-index: 3;
  outline: 0;
  box-sizing: border-box;
  overflow-y: auto;
  transform: translate3d(-100%, 0, 0);
}
@media (forced-colors: active) {
  .mat-drawer, [dir=rtl] .mat-drawer.mat-drawer-end {
    border-right: solid 1px currentColor;
  }
}
@media (forced-colors: active) {
  [dir=rtl] .mat-drawer, .mat-drawer.mat-drawer-end {
    border-left: solid 1px currentColor;
    border-right: none;
  }
}
.mat-drawer.mat-drawer-side {
  z-index: 2;
}
.mat-drawer.mat-drawer-end {
  right: 0;
  transform: translate3d(100%, 0, 0);
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
[dir=rtl] .mat-drawer {
  border-top-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-left-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  transform: translate3d(100%, 0, 0);
}
[dir=rtl] .mat-drawer.mat-drawer-end {
  border-top-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-bottom-right-radius: var(--mat-sidenav-container-shape, var(--mat-sys-corner-large));
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  left: 0;
  right: auto;
  transform: translate3d(-100%, 0, 0);
}
.mat-drawer-transition .mat-drawer {
  transition: transform 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) {
  visibility: hidden;
  box-shadow: none;
}
.mat-drawer:not(.mat-drawer-opened):not(.mat-drawer-animating) .mat-drawer-inner-container {
  display: none;
}
.mat-drawer.mat-drawer-opened.mat-drawer-opened {
  transform: none;
}

.mat-drawer-side {
  box-shadow: none;
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
}
.mat-drawer-side.mat-drawer-end {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side {
  border-left-color: var(--mat-sidenav-container-divider-color, transparent);
  border-left-width: 1px;
  border-left-style: solid;
  border-right: none;
}
[dir=rtl] .mat-drawer-side.mat-drawer-end {
  border-right-color: var(--mat-sidenav-container-divider-color, transparent);
  border-right-width: 1px;
  border-right-style: solid;
  border-left: none;
}

.mat-drawer-inner-container {
  width: 100%;
  height: 100%;
  overflow: auto;
}

.mat-sidenav-fixed {
  position: fixed;
}
`],encapsulation:2,changeDetection:0})}return t})(),$h=(()=>{class t extends Hh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav-content"]],hostAttrs:[1,"mat-drawer-content","mat-sidenav-content"],features:[ge([{provide:kr,useExisting:t}]),J],ngContentSelectors:Gh,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},encapsulation:2,changeDetection:0})}return t})(),v0=(()=>{class t extends p0{get fixedInViewport(){return this._fixedInViewport}set fixedInViewport(e){this._fixedInViewport=ct(e)}_fixedInViewport=!1;get fixedTopGap(){return this._fixedTopGap}set fixedTopGap(e){this._fixedTopGap=Yt(e)}_fixedTopGap=0;get fixedBottomGap(){return this._fixedBottomGap}set fixedBottomGap(e){this._fixedBottomGap=Yt(e)}_fixedBottomGap=0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav"]],hostAttrs:[1,"mat-drawer","mat-sidenav"],hostVars:16,hostBindings:function(i,r){i&2&&(te("tabIndex",r.mode!=="side"?"-1":null)("align",null),hr("top",r.fixedInViewport?r.fixedTopGap:null,"px")("bottom",r.fixedInViewport?r.fixedBottomGap:null,"px"),N("mat-drawer-end",r.position==="end")("mat-drawer-over",r.mode==="over")("mat-drawer-push",r.mode==="push")("mat-drawer-side",r.mode==="side")("mat-sidenav-fixed",r.fixedInViewport))},inputs:{fixedInViewport:"fixedInViewport",fixedTopGap:"fixedTopGap",fixedBottomGap:"fixedBottomGap"},exportAs:["matSidenav"],features:[ge([{provide:p0,useExisting:t}]),J],ngContentSelectors:Gh,decls:3,vars:0,consts:[["content",""],["cdkScrollable","",1,"mat-drawer-inner-container"]],template:function(i,r){i&1&&(xe(),p(0,"div",1,0),U(2),g())},dependencies:[kr],encapsulation:2,changeDetection:0})}return t})(),dT=(()=>{class t extends g0{_allDrawers=void 0;_content=void 0;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-sidenav-container"]],contentQueries:function(i,r,o){if(i&1&&wt(o,$h,5)(o,v0,5),i&2){let s;H(s=$())&&(r._content=s.first),H(s=$())&&(r._allDrawers=s)}},hostAttrs:[1,"mat-drawer-container","mat-sidenav-container"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-drawer-container-explicit-backdrop",r._backdropOverride)},exportAs:["matSidenavContainer"],features:[ge([{provide:_0,useExisting:t},{provide:g0,useExisting:t}]),J],ngContentSelectors:dU,decls:4,vars:2,consts:[[1,"mat-drawer-backdrop",3,"mat-drawer-shown"],[1,"mat-drawer-backdrop",3,"click"]],template:function(i,r){i&1&&(xe(cU),me(0,uU,1,2,"div",0),U(1),U(2,1),me(3,fU,2,0,"mat-sidenav-content")),i&2&&(he(r.hasBackdrop?0:-1),v(3),he(r._content?-1:3))},dependencies:[$h],styles:[mU],encapsulation:2,changeDetection:0})}return t})(),uT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Xi,_e,Xi]})}return t})();var gU=["*",[["mat-toolbar-row"]]],_U=["*","mat-toolbar-row"],vU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),fT=(()=>{class t{_elementRef=d(O);_platform=d(be);_document=d(G);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&wt(o,vU,5),i&2){let s;H(s=$())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(on(r.color?"mat-"+r.color:""),N("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:_U,decls:2,vars:0,template:function(i,r){i&1&&(xe(gU),U(0),U(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var mT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var vd=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new I;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Wh=class{applyChanges(n,e,i,r,o){n.forEachOperation((s,a,l)=>{let c,u;if(s.previousIndex==null){let f=i(s,a,l);c=e.createEmbeddedView(f.templateRef,f.context,f.index),u=vi.INSERTED}else l==null?(e.remove(a),u=vi.REMOVED):(c=e.get(a),e.move(c,l),u=vi.MOVED);o&&o({context:c?.context,operation:u,record:s})})}detach(){}};var hT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var bU=["*"],wU=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,CU=["unscopedContent"],DU=["text"],xU=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]],EU=["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"];var SU=new b("ListOption"),b0=(()=>{class t{_elementRef=d(O);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),IU=(()=>{class t{_elementRef=d(O);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),kU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),pT=(()=>{class t{_listOption=d(SU,{optional:!0});constructor(){}_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:4,hostBindings:function(i,r){i&2&&N("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),MU=(()=>{class t extends pT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[J]})}return t})(),w0=(()=>{class t extends pT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[J]})}return t})(),TU=new b("MAT_LIST_CONFIG"),y0=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=ct(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(ct(e))}_disabled=X(!1);_defaultOptions=d(TU,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,hostVars:1,hostBindings:function(i,r){i&2&&te("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),AU=(()=>{class t{_elementRef=d(O);_ngZone=d(z);_listBase=d(y0,{optional:!0});_platform=d(be);_hostElement;_isButtonElement;_noopAnimations=Ve();_avatars;_icons;set lines(e){this._explicitLines=Yt(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=ct(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(ct(e))}_disabled=X(!1);_subscriptions=new Se;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(tt).load(Gn);let e=d(Qc,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new bs(this,this._ngZone,this._hostElement,this._platform,d(Y)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(Et(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,contentQueries:function(i,r,o){if(i&1&&wt(o,MU,4)(o,w0,4),i&2){let s;H(s=$())&&(r._avatars=s),H(s=$())&&(r._icons=s)}},hostVars:4,hostBindings:function(i,r){i&2&&(te("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),N("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var gT=(()=>{class t extends AU{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=ct(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(i,r,o){if(i&1&&wt(o,IU,5)(o,b0,5)(o,kU,5),i&2){let s;H(s=$())&&(r._lines=s),H(s=$())&&(r._titles=s),H(s=$())&&(r._meta=s)}},viewQuery:function(i,r){if(i&1&&He(CU,5)(DU,5),i&2){let o;H(o=$())&&(r._unscopedContent=o.first),H(o=$())&&(r._itemText=o.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(i,r){i&2&&(te("aria-current",r._getAriaCurrent()),N("mdc-list-item--activated",r.activated)("mdc-list-item--with-leading-avatar",r._avatars.length!==0)("mdc-list-item--with-leading-icon",r._icons.length!==0)("mdc-list-item--with-trailing-meta",r._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("_mat-animation-noopable",r._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[J],ngContentSelectors:EU,decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(i,r){i&1&&(xe(xU),U(0),p(1,"span",1),U(2,1),U(3,2),p(4,"span",2,0),A("cdkObserveContent",function(){return r._updateItemLines(!0)}),U(6,3),g()(),U(7,4),U(8,5),F(9,"div",3))},dependencies:[xk],encapsulation:2,changeDetection:0})}return t})();var _T=(()=>{class t extends y0{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-nav-list"]],hostAttrs:["role","navigation",1,"mat-mdc-nav-list","mat-mdc-list-base","mdc-list"],exportAs:["matNavList"],features:[ge([{provide:y0,useExisting:t}]),J],ngContentSelectors:bU,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},styles:[wU],encapsulation:2,changeDetection:0})}return t})();var vT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Ra,Ba,mh,_e,hT]})}return t})();function yT(t){return Error(`Unable to find icon with the name "${t}"`)}function OU(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function bT(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function wT(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Nr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},DT=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Nr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(bt.HTML,r);if(!s)throw wT(r);let a=vs(s);return this._addSvgIconConfig(e,i,new Nr("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Nr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(bt.HTML,i);if(!o)throw wT(i);let s=vs(o);return this._addSvgIconSetConfig(e,new Nr("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(bt.RESOURCE_URL,e);if(!i)throw bT(e);let r=this._cachedIconsByUrl.get(i);return r?q(qh(r)):this._loadSvgIconFromConfig(new Nr(e,null)).pipe(st(o=>this._cachedIconsByUrl.set(i,o)),ee(o=>qh(o)))}getNamedSvgIcon(e,i=""){let r=CT(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):ul(yT(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?q(qh(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ee(i=>qh(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return q(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Hr(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(bt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),q(null)})));return fl(o).pipe(ee(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw yT(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(st(i=>e.svgText=i),ee(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?q(null):this._fetchIcon(e).pipe(st(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(vs("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(vs("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw OU();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(bt.RESOURCE_URL,i);if(!s)throw bT(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ee(c=>vs(c)),No(()=>this._inProgressUrlFetches.delete(s)),ml());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(CT(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return NU(o)?new Nr(o.url,null,o.options):new Nr(o,null)}}static \u0275fac=function(i){return new(i||t)(V(ji,8),V(fc),V(G,8),V(en))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qh(t){return t.cloneNode(!0)}function CT(t,n){return t+":"+n}function NU(t){return!!(t.url&&t.options)}var FU=["*"],PU=new b("MAT_ICON_DEFAULT_OPTIONS"),LU=new b("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(G),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),xT=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],BU=xT.map(t=>`[${t}]`).join(", "),VU=/^url\(['"]?#(.*?)['"]?\)$/,Za=(()=>{class t{_elementRef=d(O);_iconRegistry=d(DT);_location=d(LU);_errorHandler=d(en);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=Se.EMPTY;constructor(){let e=d(new _n("aria-hidden"),{optional:!0}),i=d(PU,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(BU),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)xT.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(VU):null;if(c){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(rt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(te("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),on(r.color?"mat-"+r.color:""),N("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",j],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:FU,decls:1,vars:0,template:function(i,r){i&1&&(xe(),U(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Xa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var jU=["sidenav"],zU=()=>({exact:!0});function UU(t,n){if(t&1){let e=Ze();p(0,"button",15),A("click",function(){Ae(e),W();let r=gt(2);return Re(r.toggle())}),p(1,"mat-icon"),y(2,"menu"),g()()}}var Qh=class t{constructor(n,e,i){this.authService=n;this.router=e;this.breakpointObserver=i}sidenav;isMobile=!1;breakpointSubscription;ngOnInit(){this.breakpointSubscription=this.breakpointObserver.observe([Bc.Handset,Bc.TabletPortrait]).subscribe(n=>{this.isMobile=n.matches,!this.isMobile&&this.sidenav&&this.sidenav.open()})}ngOnDestroy(){this.breakpointSubscription&&this.breakpointSubscription.unsubscribe()}closeOnMobile(){this.isMobile&&this.sidenav.close()}logout(){this.authService.logout(),this.router.navigate(["/login"])}static \u0275fac=function(e){return new(e||t)(R(bn),R(mt),R(_s))};static \u0275cmp=T({type:t,selectors:[["app-dashboard-layout"]],viewQuery:function(e,i){if(e&1&&He(jU,5),e&2){let r;H(r=$())&&(i.sidenav=r.first)}},decls:31,vars:5,consts:[["sidenav",""],[1,"sidenav-container"],[1,"sidenav",3,"mode","opened"],[1,"sidenav-header"],["mat-list-item","","routerLink","/dashboard","routerLinkActive","active",3,"click","routerLinkActiveOptions"],["matListItemIcon",""],["matListItemTitle",""],["mat-list-item","","routerLink","/dashboard/products","routerLinkActive","active",3,"click"],["color","primary",1,"toolbar"],["mat-icon-button","","title","Menu",3,"click",4,"ngIf"],[1,"app-title"],[1,"spacer"],["mat-icon-button","","routerLink","/dashboard/profile","title","Profile"],["mat-icon-button","","title","Logout",3,"click"],[1,"content-wrapper"],["mat-icon-button","","title","Menu",3,"click"]],template:function(e,i){e&1&&(p(0,"mat-sidenav-container",1)(1,"mat-sidenav",2,0)(3,"div",3)(4,"h2"),y(5,"Dashboard"),g()(),p(6,"mat-nav-list")(7,"a",4),A("click",function(){return i.closeOnMobile()}),p(8,"mat-icon",5),y(9,"dashboard"),g(),p(10,"div",6),y(11,"Overview"),g()(),p(12,"a",7),A("click",function(){return i.closeOnMobile()}),p(13,"mat-icon",5),y(14,"inventory"),g(),p(15,"div",6),y(16,"Products"),g()()()(),p(17,"mat-sidenav-content")(18,"mat-toolbar",8),se(19,UU,3,0,"button",9),p(20,"span",10),y(21,"App"),g(),F(22,"span",11),p(23,"button",12)(24,"mat-icon"),y(25,"account_circle"),g()(),p(26,"button",13),A("click",function(){return i.logout()}),p(27,"mat-icon"),y(28,"logout"),g()()(),p(29,"div",14),F(30,"router-outlet"),g()()()),e&2&&(v(),k("mode",i.isMobile?"over":"side")("opened",!i.isMobile),v(6),k("routerLinkActiveOptions",pr(4,zU)),v(12),k("ngIf",i.isMobile))},dependencies:[Rt,ui,$y,cs,Un,zy,uT,v0,dT,$h,mT,fT,vT,_T,gT,w0,b0,Xa,Za,Nt,Cs],styles:[".sidenav-container[_ngcontent-%COMP%]{height:100vh}.sidenav[_ngcontent-%COMP%]{width:250px;background-color:#fff;border-right:1px solid #e0e0e0}.sidenav-header[_ngcontent-%COMP%]{padding:24px 16px;border-bottom:1px solid #f0f0f0;margin-bottom:8px}.sidenav-header[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin:0;font-weight:600;color:var(--primary-color)}.active[_ngcontent-%COMP%]{background-color:var(--secondary-color)!important;color:var(--primary-color)!important}.spacer[_ngcontent-%COMP%]{flex:1 1 auto}.toolbar[_ngcontent-%COMP%]{box-shadow:0 2px 4px #0000001a;z-index:2;position:relative}.app-title[_ngcontent-%COMP%]{margin-left:8px}.content-wrapper[_ngcontent-%COMP%]{padding:24px;height:calc(100vh - 112px);overflow-y:auto}@media(max-width:600px){.content-wrapper[_ngcontent-%COMP%]{padding:16px}.app-title[_ngcontent-%COMP%]{font-size:16px}}"]})};var Ja=class t{constructor(n){this.http=n}apiUrl=`${Dr.apiUrl}/api/profile`;getProfile(){return this.http.get(`${this.apiUrl}/me`)}updateProfile(n){return this.http.put(`${this.apiUrl}/update`,n)}changePassword(n){return this.http.post(`${this.apiUrl}/change-password`,n)}uploadProfilePicture(n){let e=new FormData;return e.append("file",n,n.name),this.http.post(`${this.apiUrl}/upload-picture`,e)}getBusinessProfile(){return this.http.get(`${this.apiUrl}/business`)}updateBusinessProfile(n){return this.http.put(`${this.apiUrl}/business`,n)}uploadBusinessLogo(n){let e=new FormData;return e.append("file",n,n.name),this.http.post(`${this.apiUrl}/business/upload-logo`,e)}static \u0275fac=function(e){return new(e||t)(V(ji))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var el=class t{constructor(n){this.http=n}apiUrl=`${Dr.apiUrl}/api/product`;getProducts(){return this.http.get(this.apiUrl)}getProduct(n){return this.http.get(`${this.apiUrl}/${n}`)}addProduct(n){return this.http.post(this.apiUrl,n)}updateProduct(n,e){return this.http.put(`${this.apiUrl}/${n}`,e)}deleteProduct(n){return this.http.delete(`${this.apiUrl}/${n}`)}static \u0275fac=function(e){return new(e||t)(V(ji))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Yh=class t{constructor(n,e,i){this.profileService=n;this.productService=e;this.cdr=i}profile=null;hasBusinessProfile=!1;totalProducts=0;ngOnInit(){this.profileService.getProfile().subscribe(n=>{this.profile=n,this.cdr.detectChanges()}),this.profileService.getBusinessProfile().subscribe(n=>{this.hasBusinessProfile=!!n,this.cdr.detectChanges()}),this.productService.getProducts().subscribe({next:n=>{this.totalProducts=n?n.length:0,this.cdr.detectChanges()},error:n=>{console.error("Error fetching products:",n)}})}static \u0275fac=function(e){return new(e||t)(R(Ja),R(el),R(Ce))};static \u0275cmp=T({type:t,selectors:[["app-overview"]],decls:24,vars:4,consts:[[1,"overview-container"],[1,"subtitle"],[1,"stats-grid"],[1,"stat-card"],[1,"stat-header"],["color","primary"],[1,"stat-value"],["color","accent"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"h1"),y(2),g(),p(3,"p",1),y(4,"Here's what's happening with your account today."),g(),p(5,"div",2)(6,"mat-card",3)(7,"mat-card-content")(8,"div",4)(9,"mat-icon",5),y(10,"inventory_2"),g(),p(11,"h3"),y(12,"Total Products"),g()(),p(13,"div",6),y(14),g()()(),p(15,"mat-card",3)(16,"mat-card-content")(17,"div",4)(18,"mat-icon",7),y(19,"business"),g(),p(20,"h3"),y(21,"Business Profile"),g()(),p(22,"div",6),y(23),g()()()()()),e&2&&(v(2),Oe("Welcome back, ",(i.profile==null?null:i.profile.fullName)||"User","!"),v(12),Nf("",i.totalProducts," ",i.totalProducts===1?"Product":"Products"),v(9),Pt(i.hasBusinessProfile?"Active":"Not Setup"))},dependencies:[Rt,Zi,Ki,kh,Xa,Za],styles:[".overview-container[_ngcontent-%COMP%]{max-width:1200px;margin:0 auto}h1[_ngcontent-%COMP%]{font-size:28px;margin-bottom:8px;color:var(--primary-color)}.subtitle[_ngcontent-%COMP%]{color:var(--text-secondary);margin-bottom:32px}.stats-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr));gap:24px}.stat-card[_ngcontent-%COMP%]{padding:16px;transition:transform .2s ease}.stat-card[_ngcontent-%COMP%]:hover{transform:translateY(-4px)}.stat-header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:12px;margin-bottom:16px}.stat-header[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0;font-size:18px;font-weight:500;color:var(--text-secondary)}.stat-header[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{transform:scale(1.2)}.stat-value[_ngcontent-%COMP%]{font-size:24px;font-weight:600;color:var(--text-primary)}"]})};var HU=["switch"],$U=["*"];function GU(t,n){t&1&&(p(0,"span",11),Ut(),p(1,"svg",13),F(2,"path",14),g(),p(3,"svg",15),F(4,"path",16),g()())}var WU=new b("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),Kh=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},D0=(()=>{class t{_elementRef=d(O);_focusMonitor=d(Mn);_changeDetectorRef=d(Ce);defaults=d(WU);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new Kh(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Ve();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new Q;toggleChange=new Q;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(tt).load(Gn);let e=d(new _n("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(We).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new Kh(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&He(HU,5),i&2){let o;H(o=$())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(qt("id",r.id),te("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),on(r.color?"mat-"+r.color:""),N("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",j],color:"color",disabled:[2,"disabled","disabled",j],disableRipple:[2,"disableRipple","disableRipple",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:an(e)],checked:[2,"checked","checked",j],hideIcon:[2,"hideIcon","hideIcon",j],disabledInteractive:[2,"disabledInteractive","disabledInteractive",j]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[ge([{provide:ws,useExisting:kt(()=>t),multi:!0},{provide:Er,useExisting:t,multi:!0}]),Pe],ngContentSelectors:$U,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(xe(),p(0,"div",1)(1,"button",2,0),A("click",function(){return r._handleClick()}),F(3,"div",3)(4,"span",4),p(5,"span",5)(6,"span",6)(7,"span",7),F(8,"span",8),g(),p(9,"span",9),F(10,"span",10),g(),me(11,GU,5,0,"span",11),g()()(),p(12,"label",12),A("click",function(s){return s.stopPropagation()}),U(13),g()()),i&2){let o=gt(2);k("labelPosition",r.labelPosition),v(),N("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),k("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),te("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),v(9),k("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),v(),he(r.hideIcon?-1:11),v(),k("for",r.buttonId),te("id",r._labelId)}},dependencies:[Pa,ph],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),IT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[D0,_e]})}return t})();var Zh=class t{constructor(n,e,i){this.fb=n;this.profileService=e;this.snackBar=i;this.profileForm=this.fb.group({fullName:["",le.required],phoneNumber:[""],isOtpEnabled:[!1]}),this.passwordForm=this.fb.group({currentPassword:["",le.required],newPassword:["",[le.required,le.minLength(6)]]}),this.businessForm=this.fb.group({companyName:["",le.required],address:[""],taxNumber:[""],phoneNumber:[""]})}profileForm;passwordForm;businessForm;profilePictureUrl=null;businessLogoUrl=null;ngOnInit(){this.profileService.getProfile().subscribe(n=>{n&&(this.profileForm.patchValue({fullName:n.fullName,phoneNumber:n.phoneNumber,isOtpEnabled:n.isOtpEnabled}),this.profilePictureUrl=n.profilePictureUrl)}),this.profileService.getBusinessProfile().subscribe(n=>{n&&(this.businessForm.patchValue({companyName:n.companyName,address:n.address,taxNumber:n.taxNumber,phoneNumber:n.phoneNumber}),this.businessLogoUrl=n.logoUrl)})}updateProfile(){this.profileForm.valid&&this.profileService.updateProfile(this.profileForm.value).subscribe({next:()=>{this.snackBar.open("Profile updated","Close",{duration:3e3}),this.profileForm.markAsPristine()},error:()=>this.snackBar.open("Update failed","Close",{duration:3e3})})}changePassword(){this.passwordForm.valid&&this.profileService.changePassword(this.passwordForm.value).subscribe({next:()=>{this.snackBar.open("Password changed successfully","Close",{duration:3e3}),this.passwordForm.reset()},error:n=>{let e="Password change failed";n.error&&(n.error.message?e=n.error.message:Array.isArray(n.error)&&n.error.length>0&&(e=n.error.map(i=>i.description).join(" "))),this.snackBar.open(e,"Close",{duration:5e3})}})}updateBusinessProfile(){this.businessForm.valid&&this.profileService.updateBusinessProfile(this.businessForm.value).subscribe({next:()=>{this.snackBar.open("Business profile updated","Close",{duration:3e3}),this.businessForm.markAsPristine()},error:()=>this.snackBar.open("Update failed","Close",{duration:3e3})})}getProfileImageUrl(){return this.profilePictureUrl?this.profilePictureUrl.startsWith("http")?this.profilePictureUrl:`${Dr.apiUrl}${this.profilePictureUrl}`:"https://ui-avatars.com/api/?name=User&background=random"}getBusinessLogoUrl(){return this.businessLogoUrl?this.businessLogoUrl.startsWith("http")?this.businessLogoUrl:`${Dr.apiUrl}${this.businessLogoUrl}`:"https://ui-avatars.com/api/?name=Business&background=random"}onProfilePictureSelected(n){let e=n.target.files[0];e&&this.profileService.uploadProfilePicture(e).subscribe({next:i=>{this.profilePictureUrl=i.url,this.snackBar.open("Profile picture updated","Close",{duration:3e3})},error:()=>this.snackBar.open("Failed to upload picture","Close",{duration:3e3})})}onBusinessLogoSelected(n){let e=n.target.files[0];e&&this.profileService.uploadBusinessLogo(e).subscribe({next:i=>{this.businessLogoUrl=i.url,this.snackBar.open("Business logo updated","Close",{duration:3e3})},error:()=>this.snackBar.open("Failed to upload logo","Close",{duration:3e3})})}static \u0275fac=function(e){return new(e||t)(R(Qi),R(Ja),R(wi))};static \u0275cmp=T({type:t,selectors:[["app-profile-manager"]],decls:77,vars:7,consts:[["profilePicInput",""],["businessLogoInput",""],[1,"profile-container"],[1,"forms-grid"],[1,"profile-card"],[1,"profile-picture-container"],["alt","Profile Picture",1,"profile-avatar",3,"src"],["mat-stroked-button","","color","primary","type","button",3,"click"],["type","file","accept","image/*",1,"file-input",3,"change"],[3,"ngSubmit","formGroup"],["appearance","outline",1,"form-field-full"],["matInput","","formControlName","fullName"],["matInput","","formControlName","phoneNumber"],[1,"toggle-container"],["formControlName","isOtpEnabled","color","primary"],["mat-flat-button","","type","submit",1,"primary-button",3,"disabled"],["matInput","","formControlName","currentPassword","type","password"],["matInput","","formControlName","newPassword","type","password"],["mat-flat-button","","color","accent","type","submit"],["alt","Business Logo",1,"business-logo",3,"src"],["matInput","","formControlName","companyName"],["matInput","","formControlName","address"],["matInput","","formControlName","taxNumber"]],template:function(e,i){if(e&1){let r=Ze();p(0,"div",2)(1,"h2"),y(2,"Profile Management"),g(),p(3,"div",3)(4,"mat-card",4)(5,"mat-card-header")(6,"mat-card-title"),y(7,"Personal Details"),g()(),p(8,"mat-card-content")(9,"div",5),F(10,"img",6),p(11,"div")(12,"button",7),A("click",function(){Ae(r);let s=gt(15);return Re(s.click())}),y(13,"Change Picture"),g(),p(14,"input",8,0),A("change",function(s){return i.onProfilePictureSelected(s)}),g()()(),p(16,"form",9),A("ngSubmit",function(){return i.updateProfile()}),p(17,"mat-form-field",10)(18,"mat-label"),y(19,"Full Name"),g(),F(20,"input",11),g(),p(21,"mat-form-field",10)(22,"mat-label"),y(23,"Phone Number"),g(),F(24,"input",12),g(),p(25,"div",13)(26,"mat-slide-toggle",14),y(27," Enable Login OTP (Two-Factor Authentication) "),g()(),p(28,"button",15),y(29,"Update Profile"),g()()()(),p(30,"mat-card",4)(31,"mat-card-header")(32,"mat-card-title"),y(33,"Change Password"),g()(),p(34,"mat-card-content")(35,"form",9),A("ngSubmit",function(){return i.changePassword()}),p(36,"mat-form-field",10)(37,"mat-label"),y(38,"Current Password"),g(),F(39,"input",16),g(),p(40,"mat-form-field",10)(41,"mat-label"),y(42,"New Password"),g(),F(43,"input",17),g(),p(44,"button",18),y(45,"Change Password"),g()()()(),p(46,"mat-card",4)(47,"mat-card-header")(48,"mat-card-title"),y(49,"Business Details"),g()(),p(50,"mat-card-content")(51,"div",5),F(52,"img",19),p(53,"div")(54,"button",7),A("click",function(){Ae(r);let s=gt(57);return Re(s.click())}),y(55,"Change Logo"),g(),p(56,"input",8,1),A("change",function(s){return i.onBusinessLogoSelected(s)}),g()()(),p(58,"form",9),A("ngSubmit",function(){return i.updateBusinessProfile()}),p(59,"mat-form-field",10)(60,"mat-label"),y(61,"Company Name"),g(),F(62,"input",20),g(),p(63,"mat-form-field",10)(64,"mat-label"),y(65,"Address"),g(),F(66,"input",21),g(),p(67,"mat-form-field",10)(68,"mat-label"),y(69,"Tax Number"),g(),F(70,"input",22),g(),p(71,"mat-form-field",10)(72,"mat-label"),y(73,"Business Phone"),g(),F(74,"input",12),g(),p(75,"button",15),y(76,"Update Business"),g()()()()()()}e&2&&(v(10),k("src",i.getProfileImageUrl(),ql),v(6),k("formGroup",i.profileForm),v(12),k("disabled",i.profileForm.pristine),v(7),k("formGroup",i.passwordForm),v(17),k("src",i.getBusinessLogoUrl(),ql),v(6),k("formGroup",i.businessForm),v(17),k("disabled",i.businessForm.pristine))},dependencies:[Rt,Yi,qi,Wn,Gi,Wi,dn,_i,Zi,Ki,kh,xM,DM,Bt,Kt,un,Qn,qn,Nt,wn,IT,D0],styles:[".profile-container[_ngcontent-%COMP%]{max-width:1200px;margin:0 auto}.profile-picture-container[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;margin-bottom:24px}.profile-avatar[_ngcontent-%COMP%]{width:80px;height:80px;border-radius:50%;object-fit:cover;background-color:#f0f0f0;border:1px solid #e0e0e0}.business-logo[_ngcontent-%COMP%]{width:80px;height:80px;border-radius:8px;object-fit:cover;background-color:#f0f0f0;border:1px solid #e0e0e0}.file-input[_ngcontent-%COMP%]{display:none}h2[_ngcontent-%COMP%]{margin-bottom:24px;color:var(--primary-color)}.toggle-container[_ngcontent-%COMP%]{margin-top:8px;margin-bottom:24px;padding:8px 0}.forms-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,400px),1fr));gap:24px}.profile-card[_ngcontent-%COMP%]{padding:16px}.form-field-full[_ngcontent-%COMP%]{width:100%;margin-top:16px}mat-card-title[_ngcontent-%COMP%]{font-size:20px;margin-bottom:16px}button[_ngcontent-%COMP%]{margin-top:8px}"]})};var QU=[[["caption"]],[["colgroup"],["col"]],"*"],YU=["caption","colgroup, col","*"];function KU(t,n){t&1&&U(0,2)}function ZU(t,n){t&1&&(p(0,"thead",0),Wt(1,1),g(),p(2,"tbody",0),Wt(3,2)(4,3),g(),p(5,"tfoot",0),Wt(6,4),g())}function XU(t,n){t&1&&Wt(0,1)(1,2)(2,3)(3,4)}var Ci=new b("CDK_TABLE");var ep=(()=>{class t{template=d(Xe);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkCellDef",""]]})}return t})(),tp=(()=>{class t{template=d(Xe);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkHeaderCellDef",""]]})}return t})(),TT=(()=>{class t{template=d(Xe);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkFooterCellDef",""]]})}return t})(),yo=(()=>{class t{_table=d(Ci,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&wt(o,ep,5)(o,tp,5)(o,TT,5),i&2){let s;H(s=$())&&(r.cell=s.first),H(s=$())&&(r.headerCell=s.first),H(s=$())&&(r.footerCell=s.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",j],stickyEnd:[2,"stickyEnd","stickyEnd",j]}})}return t})(),Jh=class{constructor(n,e){e.nativeElement.classList.add(...n._columnCssClassName)}},AT=(()=>{class t extends Jh{constructor(){super(d(yo),d(O))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[J]})}return t})();var RT=(()=>{class t extends Jh{constructor(){let e=d(yo),i=d(O);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[J]})}return t})();var E0=(()=>{class t{template=d(Xe);_differs=d(Li);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof bd?e.headerCell.template:this instanceof S0?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,features:[Pe]})}return t})(),bd=(()=>{class t extends E0{_table=d(Ci,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Xe),d(Li))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",j]},features:[J,Pe]})}return t})(),S0=(()=>{class t extends E0{_table=d(Ci,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(Xe),d(Li))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",j]},features:[J,Pe]})}return t})(),np=(()=>{class t extends E0{_table=d(Ci,{optional:!0});when;constructor(){super(d(Xe),d(Li))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[J]})}return t})(),Ss=(()=>{class t{_viewContainer=d(ot);cells;context;static mostRecentCellOutlet=null;constructor(){t.mostRecentCellOutlet=this}ngOnDestroy(){t.mostRecentCellOutlet===this&&(t.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","cdkCellOutlet",""]]})}return t})(),I0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Wt(0,0)},dependencies:[Ss],encapsulation:2})}return t})();var k0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Wt(0,0)},dependencies:[Ss],encapsulation:2})}return t})(),ip=(()=>{class t{templateRef=d(Xe);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["ng-template","cdkNoDataRow",""]]})}return t})(),kT=["top","bottom","left","right"],x0=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,e,i=!0,r=!0,o,s,a){this._isNativeHtmlTable=n,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=s,this._tableInjector=a,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(n,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let i=[];for(let r of n)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));Ke({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(n,e,i,r=!0,o=!0){if(!n.length||!this._isBrowser||!(e.some(E=>E)||i.some(E=>E))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let s=n[0],a=s.children.length,l=this.direction==="rtl",c=l?"right":"left",u=l?"left":"right",f=e.lastIndexOf(!0),h=i.indexOf(!0),m,_,D;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...e],stickyEndStates:[...i]}),Ke({earlyRead:()=>{m=this._getCellWidths(s,r),_=this._getStickyStartColumnPositions(m,e),D=this._getStickyEndColumnPositions(m,i)},write:()=>{for(let E of n)for(let M=0;M<a;M++){let Z=E.children[M];e[M]&&this._addStickyStyle(Z,c,_[M],M===f),i[M]&&this._addStickyStyle(Z,u,D[M],M===h)}this._positionListener&&m.some(E=>!!E)&&(this._positionListener.stickyColumnsUpdated({sizes:f===-1?[]:m.slice(0,f+1).map((E,M)=>e[M]?E:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:h===-1?[]:m.slice(h).map((E,M)=>i[M+h]?E:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,e,i){if(!this._isBrowser)return;let r=i==="bottom"?n.slice().reverse():n,o=i==="bottom"?e.slice().reverse():e,s=[],a=[],l=[];Ke({earlyRead:()=>{for(let c=0,u=0;c<r.length;c++){if(!o[c])continue;s[c]=u;let f=r[c];l[c]=this._isNativeHtmlTable?Array.from(f.children):[f];let h=this._retrieveElementSize(f).height;u+=h,a[c]=h}},write:()=>{let c=o.lastIndexOf(!0);for(let u=0;u<r.length;u++){if(!o[u])continue;let f=s[u],h=u===c;for(let m of l[u])this._addStickyStyle(m,i,f,h)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:a,offsets:s,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:a,offsets:s,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,e){this._isNativeHtmlTable&&Ke({write:()=>{let i=n.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,e){if(!n.classList.contains(this._stickCellCss))return;for(let r of e)n.style[r]="",n.classList.remove(this._borderCellCss[r]);kT.some(r=>e.indexOf(r)===-1&&n.style[r])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,e,i,r){n.classList.add(this._stickCellCss),r&&n.classList.add(this._borderCellCss[e]),n.style[e]=`${i}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of kT)n.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(n,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=n.children;for(let o=0;o<r.length;o++){let s=r[o];i.push(this._retrieveElementSize(s).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(n,e){let i=[],r=0;for(let o=0;o<n.length;o++)e[o]&&(i[o]=r,r+=n[o]);return i}_getStickyEndColumnPositions(n,e){let i=[],r=0;for(let o=n.length;o>0;o--)e[o]&&(i[o]=r,r+=n[o]);return i}_retrieveElementSize(n){let e=this._elemSizeCache.get(n);if(e)return e;let i=n.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(n,r),this._resizeObserver.observe(n,{box:"border-box"})),r}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let e=new Set(n);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(n){let e=!1;for(let i of n){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&JU(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function JU(t){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>t.classList.contains(n))}var yd=new b("STICKY_POSITIONING_LISTENER");var M0=(()=>{class t{viewContainer=d(ot);elementRef=d(O);constructor(){let e=d(Ci);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","rowOutlet",""]]})}return t})(),T0=(()=>{class t{viewContainer=d(ot);elementRef=d(O);constructor(){let e=d(Ci);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","headerRowOutlet",""]]})}return t})(),A0=(()=>{class t{viewContainer=d(ot);elementRef=d(O);constructor(){let e=d(Ci);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","footerRowOutlet",""]]})}return t})(),R0=(()=>{class t{viewContainer=d(ot);elementRef=d(O);constructor(){let e=d(Ci);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","noDataRowOutlet",""]]})}return t})(),O0=(()=>{class t{_differs=d(Li);_changeDetectorRef=d(Ce);_elementRef=d(O);_dir=d(Dt,{optional:!0});_platform=d(be);_viewRepeater;_viewportRuler=d(Yn);_injector=d(Y);_virtualScrollViewport=d(GM,{optional:!0,host:!0});_positionListener=d(yd,{optional:!0})||d(yd,{optional:!0,skipSelf:!0});_document=d(G);_data;_renderedRange;_onDestroy=new I;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new I;_footerRowStickyUpdates=new I;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new I;_dataStream=new I;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new Q;viewChange=new qe({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){d(new _n("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(ue(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new Th:new Wh,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),dd(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,s)=>this._getEmbeddedViewArgs(r.item,s),r=>r.item.data,r=>{r.operation===vi.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=MT(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=MT(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,s)=>{this._addStickyColumnStyles([o],this._headerRowDefs[s])}),this._rowDefs.forEach(o=>{let s=[];for(let a=0;a<i.length;a++)this._renderRows[a].rowDef===o&&s.push(i[a]);this._addStickyColumnStyles(s,o)}),r.forEach((o,s)=>{this._addStickyColumnStyles([o],this._footerRowDefs[s])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let s=this._data[o],a=this._getRenderRowsForData(s,o,r.get(s));this._cachedRenderRowsMap.has(s)||this._cachedRenderRowsMap.set(s,new WeakMap);for(let l=0;l<a.length;l++){let c=a[l],u=this._cachedRenderRowsMap.get(c.data);u.has(c.rowDef)?u.get(c.rowDef).push(c):u.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(s=>{let a=r&&r.has(s)?r.get(s):[];if(a.length){let l=a.shift();return l.dataIndex=i,l}else return{data:e,rowDef:s,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),Xh(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=Xh(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=Xh(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=Xh(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(s,a)=>{let l=!!a.getColumnsDiff();return s||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],dd(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;dd(this.dataSource)?e=this.dataSource.connect(this):Ro(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=q(this.dataSource)),this._renderChangeSubscription=hn([e,this.viewChange]).pipe(ue(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(a=>{let l=this._columnDefsByName.get(a);return l}),o=r.map(a=>a.sticky),s=r.map(a=>a.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,s,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(s=>s.when&&s.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let s=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),s}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))Ss.mostRecentCellOutlet&&Ss.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let s=e.get(i).context;s.count=r,s.first=i===0,s.last=i===r-1,s.even=i%2===0,s.odd=!s.even,this.multiTemplateDataRows?(s.dataIndex=this._renderRows[i].dataIndex,s.renderIndex=i):s.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new x0(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:q()).pipe(ue(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?zd:Bd;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(Bs(0,i),ue(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),hn([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(ue(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a){let l=o.offsets[s],c=r!==0?Math.max(r-l,l):-l;for(let u of a)u.style.top=`${-c}px`}}}),hn([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(ue(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a)for(let l of a)l.style.bottom=`${r+o.offsets[s]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),s=o.rootNodes[0];if(o.rootNodes.length===1&&s?.nodeType===this._document.ELEMENT_NODE){s.setAttribute("role","row"),s.classList.add(...e._contentClassNames);let a=s.querySelectorAll(e._cellSelector);for(let l=0;l<a.length;l++)a[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let s=e.start-r.start,a=e.end-e.start,l,c;for(let h=0;h<a;h++){let m=o.get(h+s);if(m&&m.rootNodes.length){l=c=m.rootNodes[0];break}}for(let h=a-1;h>-1;h--){let m=o.get(h+s);if(m&&m.rootNodes.length){c=m.rootNodes[m.rootNodes.length-1];break}}let u=l?.getBoundingClientRect?.(),f=c?.getBoundingClientRect?.();return u&&f?f.bottom-u.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&wt(o,ip,5)(o,yo,5)(o,np,5)(o,bd,5)(o,S0,5),i&2){let s;H(s=$())&&(r._noDataRow=s.first),H(s=$())&&(r._contentColumnDefs=s),H(s=$())&&(r._contentRowDefs=s),H(s=$())&&(r._contentHeaderRowDefs=s),H(s=$())&&(r._contentFooterRowDefs=s)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&N("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",j],fixedLayout:[2,"fixedLayout","fixedLayout",j],recycleRows:[2,"recycleRows","recycleRows",j]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[ge([{provide:Ci,useExisting:t},{provide:yd,useValue:null}])],ngContentSelectors:YU,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(xe(QU),U(0),U(1,1),me(2,KU,1,0),me(3,ZU,7,0)(4,XU,4,0)),i&2&&(v(2),he(r._isServer?2:-1),v(),he(r._isNativeHtmlTable?3:4))},dependencies:[T0,M0,R0,A0],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return t})();function Xh(t,n){return t.concat(Array.from(n))}function MT(t,n){let e=n.toUpperCase(),i=t.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var OT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[ud]})}return t})();var eH=[[["caption"]],[["colgroup"],["col"]],"*"],tH=["caption","colgroup, col","*"];function nH(t,n){t&1&&U(0,2)}function iH(t,n){t&1&&(p(0,"thead",0),Wt(1,1),g(),p(2,"tbody",2),Wt(3,3)(4,4),g(),p(5,"tfoot",0),Wt(6,5),g())}function rH(t,n){t&1&&Wt(0,1)(1,3)(2,4)(3,5)}var NT=(()=>{class t extends O0{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[ge([{provide:O0,useExisting:t},{provide:Ci,useExisting:t},{provide:yd,useValue:null}]),J],ngContentSelectors:tH,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(xe(eH),U(0),U(1,1),me(2,nH,1,0),me(3,iH,7,0)(4,rH,4,0)),i&2&&(v(2),he(r._isServer?2:-1),v(),he(r._isNativeHtmlTable?3:4))},dependencies:[T0,M0,R0,A0],styles:[`.mat-mdc-table-sticky {
  position: sticky !important;
}

mat-table {
  display: block;
}

mat-header-row {
  min-height: var(--mat-table-header-container-height, 56px);
}

mat-row {
  min-height: var(--mat-table-row-item-container-height, 52px);
}

mat-footer-row {
  min-height: var(--mat-table-footer-container-height, 52px);
}

mat-row, mat-header-row, mat-footer-row {
  display: flex;
  border-width: 0;
  border-bottom-width: 1px;
  border-style: solid;
  align-items: center;
  box-sizing: border-box;
}

mat-cell:first-of-type, mat-header-cell:first-of-type, mat-footer-cell:first-of-type {
  padding-left: 24px;
}
[dir=rtl] mat-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:first-of-type:not(:only-of-type) {
  padding-left: 0;
  padding-right: 24px;
}
mat-cell:last-of-type, mat-header-cell:last-of-type, mat-footer-cell:last-of-type {
  padding-right: 24px;
}
[dir=rtl] mat-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:last-of-type:not(:only-of-type) {
  padding-right: 0;
  padding-left: 24px;
}

mat-cell, mat-header-cell, mat-footer-cell {
  flex: 1;
  display: flex;
  align-items: center;
  overflow: hidden;
  word-wrap: break-word;
  min-height: inherit;
}

.mat-mdc-table {
  min-width: 100%;
  border: 0;
  border-spacing: 0;
  table-layout: auto;
  white-space: normal;
  background-color: var(--mat-table-background-color, var(--mat-sys-surface));
}

.mat-table-fixed-layout {
  table-layout: fixed;
}

.mdc-data-table__cell {
  box-sizing: border-box;
  overflow: hidden;
  text-align: start;
  text-overflow: ellipsis;
}

.mdc-data-table__cell,
.mdc-data-table__header-cell {
  padding: 0 16px;
}

.mat-mdc-header-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-header-container-height, 56px);
  color: var(--mat-table-header-headline-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-header-headline-font, var(--mat-sys-title-small-font, Roboto, sans-serif));
  line-height: var(--mat-table-header-headline-line-height, var(--mat-sys-title-small-line-height));
  font-size: var(--mat-table-header-headline-size, var(--mat-sys-title-small-size, 14px));
  font-weight: var(--mat-table-header-headline-weight, var(--mat-sys-title-small-weight, 500));
}

.mat-mdc-row {
  height: var(--mat-table-row-item-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
}

.mat-mdc-row,
.mdc-data-table__content {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-table-row-item-label-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-row-item-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-row-item-label-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-row-item-label-text-weight, var(--mat-sys-body-medium-weight));
}

.mat-mdc-footer-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-footer-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-footer-supporting-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-footer-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-footer-supporting-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-footer-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-table-footer-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}

.mat-mdc-header-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-header-headline-tracking, var(--mat-sys-title-small-tracking));
  font-weight: inherit;
  line-height: inherit;
  box-sizing: border-box;
  text-overflow: ellipsis;
  overflow: hidden;
  outline: none;
  text-align: start;
}
.mdc-data-table__row:last-child > .mat-mdc-header-cell {
  border-bottom: none;
}

.mat-mdc-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
  line-height: inherit;
}
.mdc-data-table__row:last-child > .mat-mdc-cell {
  border-bottom: none;
}

.mat-mdc-footer-cell {
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
}

mat-row.mat-mdc-row,
mat-header-row.mat-mdc-header-row,
mat-footer-row.mat-mdc-footer-row {
  border-bottom: none;
}

.mat-mdc-table tbody,
.mat-mdc-table tfoot,
.mat-mdc-table thead,
.mat-mdc-cell,
.mat-mdc-footer-cell,
.mat-mdc-header-row,
.mat-mdc-row,
.mat-mdc-footer-row,
.mat-mdc-table .mat-mdc-header-cell {
  background: inherit;
}

.mat-mdc-table mat-header-row.mat-mdc-header-row,
.mat-mdc-table mat-row.mat-mdc-row,
.mat-mdc-table mat-footer-row.mat-mdc-footer-cell {
  height: unset;
}

mat-header-cell.mat-mdc-header-cell,
mat-cell.mat-mdc-cell,
mat-footer-cell.mat-mdc-footer-cell {
  align-self: stretch;
}
`],encapsulation:2})}return t})(),FT=(()=>{class t extends ep{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matCellDef",""]],features:[ge([{provide:ep,useExisting:t}]),J]})}return t})(),PT=(()=>{class t extends tp{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matHeaderCellDef",""]],features:[ge([{provide:tp,useExisting:t}]),J]})}return t})();var LT=(()=>{class t extends yo{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[ge([{provide:yo,useExisting:t}]),J]})}return t})(),BT=(()=>{class t extends AT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[J]})}return t})();var VT=(()=>{class t extends RT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[J]})}return t})();var jT=(()=>{class t extends bd{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",j]},features:[ge([{provide:bd,useExisting:t}]),J]})}return t})();var zT=(()=>{class t extends np{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[ge([{provide:np,useExisting:t}]),J]})}return t})(),UT=(()=>{class t extends I0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[ge([{provide:I0,useExisting:t}]),J],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Wt(0,0)},dependencies:[Ss],encapsulation:2})}return t})();var HT=(()=>{class t extends k0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[ge([{provide:k0,useExisting:t}]),J],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Wt(0,0)},dependencies:[Ss],encapsulation:2})}return t})(),$T=(()=>{class t extends ip{_cellSelector="td, mat-cell, [mat-cell], .mat-cell";constructor(){super(),this._contentClassNames.push("mat-mdc-no-data-row","mat-mdc-row","mdc-data-table__row"),this._cellClassNames.push("mat-mdc-cell","mdc-data-table__cell","mat-no-data-cell")}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["ng-template","matNoDataRow",""]],features:[ge([{provide:ip,useExisting:t}]),J]})}return t})();var GT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[OT,_e]})}return t})(),oH=9007199254740991,rp=class extends cd{_data;_renderData=new qe([]);_filter=new qe("");_internalPageChanges=new I;_renderChangesSubscription=null;filteredData;get data(){return this._data.value}set data(n){n=Array.isArray(n)?n:[],this._data.next(n),this._renderChangesSubscription||this._filterData(n)}get filter(){return this._filter.value}set filter(n){this._filter.next(n),this._renderChangesSubscription||this._filterData(this.data)}get sort(){return this._sort}set sort(n){this._sort=n,this._updateChangeSubscription()}_sort;get paginator(){return this._paginator}set paginator(n){this._paginator=n,this._updateChangeSubscription()}_paginator;sortingDataAccessor=(n,e)=>{let i=n[e];if(eh(i)){let r=Number(i);return r<oH?r:i}return i};sortData=(n,e)=>{let i=e.active,r=e.direction;return!i||r==""?n:n.sort((o,s)=>{let a=this.sortingDataAccessor(o,i),l=this.sortingDataAccessor(s,i),c=typeof a,u=typeof l;c!==u&&(c==="number"&&(a+=""),u==="number"&&(l+=""));let f=0;return a!=null&&l!=null?a>l?f=1:a<l&&(f=-1):a!=null?f=1:l!=null&&(f=-1),f*(r=="asc"?1:-1)})};filterPredicate=(n,e)=>{let i=e.trim().toLowerCase();return Object.values(n).some(r=>`${r}`.toLowerCase().includes(i))};constructor(n=[]){super(),this._data=new qe(n),this._updateChangeSubscription()}_updateChangeSubscription(){let n=this._sort?Et(this._sort.sortChange,this._sort.initialized):q(null),e=this._paginator?Et(this._paginator.page,this._internalPageChanges,this._paginator.initialized):q(null),i=this._data,r=hn([i,this._filter]).pipe(ee(([a])=>this._filterData(a))),o=hn([r,n]).pipe(ee(([a])=>this._orderData(a))),s=hn([o,e]).pipe(ee(([a])=>this._pageData(a)));this._renderChangesSubscription?.unsubscribe(),this._renderChangesSubscription=s.subscribe(a=>this._renderData.next(a))}_filterData(n){return this.filteredData=this.filter==null||this.filter===""?n:n.filter(e=>this.filterPredicate(e,this.filter)),this.paginator&&this._updatePaginator(this.filteredData.length),this.filteredData}_orderData(n){return this.sort?this.sortData(n.slice(),this.sort):n}_pageData(n){if(!this.paginator)return n;let e=this.paginator.pageIndex*this.paginator.pageSize;return n.slice(e,e+this.paginator.pageSize)}_updatePaginator(n){Promise.resolve().then(()=>{let e=this.paginator;if(e&&(e.length=n,e.pageIndex>0)){let i=Math.ceil(e.length/e.pageSize)-1||0,r=Math.min(e.pageIndex,i);r!==e.pageIndex&&(e.pageIndex=r,this._internalPageChanges.next())}})}connect(){return this._renderChangesSubscription||this._updateChangeSubscription(),this._renderData}disconnect(){this._renderChangesSubscription?.unsubscribe(),this._renderChangesSubscription=null}};function aH(t,n){}var bo=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var F0=(()=>{class t extends go{_elementRef=d(O);_focusTrapFactory=d(Hc);_config;_interactivityChecker=d(Oa);_ngZone=d(z);_focusMonitor=d(Mn);_renderer=d($e);_changeDetectorRef=d(Ce);_injector=d(Y);_platform=d(be);_document=d(G);_portalOutlet;_focusTrapped=new I;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(bo,{optional:!0})||new bo,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Ke(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Ma(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Ma();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Ma()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&He(_o,7),i&2){let o;H(o=$())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&te("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[J],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&se(0,aH,0,0,"ng-template",0)},dependencies:[_o],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),wd=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new I;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ot(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},lH=new b("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(Y);return()=>Qa(t)}}),cH=new b("DialogData"),dH=new b("DefaultDialogConfig");function uH(t){let n=X(t),e=new Q;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var P0=(()=>{class t{_injector=d(Y);_defaultOptions=d(dH,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(Bh);_idGenerator=d(We);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;_ariaHiddenElements=new Map;_scrollStrategy=d(lH);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=Zn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(St(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new bo;i=w(w({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=Or(this._injector,o),a=new wd(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(rt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){N0(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){N0(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),N0(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new Ar({positionStrategy:e.positionStrategy||vo().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:bo,useValue:r},{provide:wd,useValue:i},{provide:Wa,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=F0;let l=new yi(a,r.viewContainerRef,Y.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Xe){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=w(w({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Mr(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new yi(e,o.viewContainerRef,s));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:cH,useValue:e.data},{provide:wd,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(Dt,null,{optional:!0}))&&a.push({provide:Dt,useValue:uH(e.direction)}),Y.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function N0(t,n){let e=t.length;for(;e--;)n(t[e])}var WT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({providers:[P0],imports:[bi,Tr,Gc,Tr]})}return t})();function fH(t,n){}var sp=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},L0="mdc-dialog--open",qT="mdc-dialog--opening",QT="mdc-dialog--closing",mH=150,hH=75,pH=(()=>{class t extends F0{_animationStateChanged=new Q;_animationsEnabled=!Ve();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?KT(this._config.enterAnimationDuration)??mH:0;_exitAnimationDuration=this._animationsEnabled?KT(this._config.exitAnimationDuration)??hH:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(YT,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(qT,L0)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(L0),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(L0),this._animationsEnabled?(this._hostElement.style.setProperty(YT,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(QT)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(qT,QT)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(qt("id",r._config.id),te("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),N("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[J],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(p(0,"div",0)(1,"div",1),se(2,fH,0,0,"ng-template",2),g()())},dependencies:[_o],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),YT="--mat-dialog-transition-duration";function KT(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Yt(t.substring(0,t.length-2)):t.endsWith("s")?Yt(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var op=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(op||{}),Is=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new An(1);_beforeClosed=new An(1);_result;_closeFallbackTimeout;_state=op.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(Ie(r=>r.state==="opened"),rt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(Ie(r=>r.state==="closed"),rt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Et(this.backdropClick(),this.keydownEvents().pipe(Ie(r=>r.keyCode===27&&!this.disableClose&&!Ot(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),ZT(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(Ie(i=>i.state==="closing"),rt(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=op.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=op.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function ZT(t,n,e){return t._closeInteractionType=n,t.close(e)}var B0=new b("MatMdcDialogData"),gH=new b("mat-mdc-dialog-default-options"),_H=new b("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(Y);return()=>Qa(t)}}),Cd=(()=>{class t{_defaultOptions=d(gH,{optional:!0});_scrollStrategy=d(_H);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(We);_injector=d(Y);_dialog=d(P0);_animationsDisabled=Ve();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;dialogConfigClass=sp;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=Zn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(St(void 0)));constructor(){this._dialogRefConstructor=Is,this._dialogContainerType=pH,this._dialogDataToken=B0}open(e,i){let r;i=w(w({},this._defaultOptions||new sp),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,oe(w({},i),{positionStrategy:vo(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:bo,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),XT=(()=>{class t{dialogRef=d(Is,{optional:!0});_elementRef=d(O);_dialog=d(Cd);ariaLabel;type="button";dialogResult;_matDialogClose;constructor(){}ngOnInit(){this.dialogRef||(this.dialogRef=iA(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose||e._matDialogCloseResult;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){ZT(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&A("click",function(s){return r._onButtonClick(s)}),i&2&&te("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[Pe]})}return t})(),JT=(()=>{class t{_dialogRef=d(Is,{optional:!0});_elementRef=d(O);_dialog=d(Cd);constructor(){}ngOnInit(){this._dialogRef||(this._dialogRef=iA(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t})}return t})(),eA=(()=>{class t extends JT{id=d(We).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&qt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[J]})}return t})(),tA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[yv([kr])]})}return t})(),nA=(()=>{class t extends JT{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Me(t)))(r||t)}})();static \u0275dir=S({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&N("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[J]})}return t})();function iA(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var ap=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({providers:[Cd],imports:[WT,bi,Tr,_e]})}return t})();var vH=["trigger"],yH=["panel"],bH=[[["mat-select-trigger"]],"*"],wH=["mat-select-trigger","*"];function CH(t,n){if(t&1&&(p(0,"span",4),y(1),g()),t&2){let e=W();v(),Pt(e.placeholder)}}function DH(t,n){t&1&&U(0)}function xH(t,n){if(t&1&&(p(0,"span",11),y(1),g()),t&2){let e=W(2);v(),Pt(e.triggerValue)}}function EH(t,n){if(t&1&&(p(0,"span",5),me(1,DH,1,0)(2,xH,2,1,"span",11),g()),t&2){let e=W();v(),he(e.customTrigger?1:2)}}function SH(t,n){if(t&1){let e=Ze();p(0,"div",12,1),A("keydown",function(r){Ae(e);let o=W();return Re(o._handleKeydown(r))}),U(2,1),g()}if(t&2){let e=W();on(e.panelClass),N("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),te("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var IH=new b("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(Y);return()=>Es(t)}}),kH=new b("MAT_SELECT_CONFIG"),MH=new b("MatSelectTrigger"),V0=class{source;value;constructor(n,e){this.source=n,this.value=e}},oA=(()=>{class t{_viewportRuler=d(Yn);_changeDetectorRef=d(Ce);_elementRef=d(O);_dir=d(Dt,{optional:!0});_idGenerator=d(We);_renderer=d($e);_parentFormField=d(ld,{optional:!0});ngControl=d(xr,{self:!0,optional:!0});_liveAnnouncer=d($c);_defaultOptions=d(kH,{optional:!0});_animationsDisabled=Ve();_popoverLocation;_initialized=new I;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=$k(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=Gk(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new V0(this,e)}_scrollStrategyFactory=d(IH);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new I;_errorStateTracker;stateChanges=new I;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=X(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(le.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Zn(()=>{let e=this.options;return e?e.changes.pipe(St(e),It(()=>Et(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(It(()=>this.optionSelectionChanges))});openedChange=new Q;_openedStream=this.openedChange.pipe(Ie(e=>e),ee(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),ee(()=>{}));selectionChange=new Q;valueChange=new Q;constructor(){let e=d(ps),i=d(id,{optional:!0}),r=d(dn,{optional:!0}),o=d(new _n("tabindex"),{optional:!0}),s=d(pd,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Va(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new vd(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(ue(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(ue(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(St(null),ue(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(rt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&ch(this._trackedModal,"aria-owns",i),Ab(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;ch(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!Ot(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!Ot(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ot(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof qa?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Wc(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Et(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(ue(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Et(...this.options.map(i=>i._stateChanges)).pipe(ue(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Lt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&wt(o,MH,5)(o,La,5)(o,Bb,5),i&2){let s;H(s=$())&&(r.customTrigger=s.first),H(s=$())&&(r.options=s),H(s=$())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&He(vH,5)(yH,5)(Vh,5),i&2){let o;H(o=$())&&(r.trigger=o.first),H(o=$())&&(r.panel=o.first),H(o=$())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&A("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(te("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),N("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",j],disableRipple:[2,"disableRipple","disableRipple",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:an(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",j],placeholder:"placeholder",required:[2,"required","required",j],multiple:[2,"multiple","multiple",j],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",j],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",an],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",j]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[ge([{provide:ad,useExisting:t},{provide:Lb,useExisting:t}]),Pe],ngContentSelectors:wH,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(xe(bH),p(0,"div",2,0),A("click",function(){return r.open()}),p(3,"div",3),me(4,CH,2,1,"span",4)(5,EH,3,1,"span",5),g(),p(6,"div",6)(7,"div",7),Ut(),p(8,"svg",8),F(9,"path",9),g()()()(),se(10,SH,3,16,"ng-template",10),A("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=gt(1);v(3),te("id",r._valueId),v(),he(r.empty?4:5),v(6),k("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[qa,Vh],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var sA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[bi,Vb,_e,Xi,Bt,Vb]})}return t})();var TH=["tooltip"],AH=20;var RH=new b("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(Y);return()=>Es(t,{scrollThrottle:AH})}}),OH=new b("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var aA="tooltip-panel",NH={passive:!0},FH=8,PH=8,LH=24,BH=200,lA=(()=>{class t{_elementRef=d(O);_ngZone=d(z);_platform=d(be);_ariaDescriber=d(dh);_focusMonitor=d(Mn);_dir=d(Dt);_injector=d(Y);_viewContainerRef=d(ot);_mediaMatcher=d(ka);_document=d(G);_renderer=d($e);_animationsDisabled=Ve();_defaultOptions=d(OH,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=VH;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=ct(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=ct(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Yt(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Yt(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new I;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=FH}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(ue(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new yi(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(ue(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof O)return this._overlayRef;this._detach()}let i=this._injector.get(Ir).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${aA}`,o=hd(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(ue(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Or(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(RH)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(ue(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(ue(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(ue(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(ue(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(w(w({},r.main),o.main)),this._addOffset(w(w({},r.fallback),o.fallback))])}_addOffset(e){let i=PH,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),Ke(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${aA}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,NH))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||Ke({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Ot(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=S({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&N("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),VH=(()=>{class t{_changeDetectorRef=d(Ce);_elementRef=d(O);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Ve();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new I;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>LH&&e.width>=BH}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&He(TH,7),i&2){let o;H(o=$())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&A("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Tt(0,"div",1,0),Tf("animationend",function(s){return r._handleAnimationEnd(s)}),Tt(2,"div",2),y(3),At()()),i&2&&(on(r.tooltipClass),N("mdc-tooltip--multiline",r._isMultiline),v(3),Pt(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return t})();var cA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Gc,bi,_e,Xi]})}return t})();function jH(t,n){if(t&1&&(p(0,"mat-option",17),y(1),g()),t&2){let e=n.$implicit;k("value",e),v(),Oe(" ",e," ")}}function zH(t,n){if(t&1){let e=Ze();p(0,"mat-form-field",14)(1,"mat-select",16,0),A("selectionChange",function(r){Ae(e);let o=W(2);return Re(o._changePageSize(r.value))}),Sv(3,jH,2,2,"mat-option",17,Ev),g(),p(5,"div",18),A("click",function(){Ae(e);let r=gt(2);return Re(r.open())}),g()()}if(t&2){let e=W(2);k("appearance",e._formFieldAppearance)("color",e.color),v(),k("value",e.pageSize)("disabled",e.disabled),kf("aria-labelledby",e._pageSizeLabelId),k("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),v(2),Iv(e._displayedPageSizeOptions)}}function UH(t,n){if(t&1&&(p(0,"div",15),y(1),g()),t&2){let e=W(2);v(),Pt(e.pageSize)}}function HH(t,n){if(t&1&&(p(0,"div",3)(1,"div",13),y(2),g(),me(3,zH,6,7,"mat-form-field",14),me(4,UH,2,1,"div",15),g()),t&2){let e=W();v(),te("id",e._pageSizeLabelId),v(),Oe(" ",e._intl.itemsPerPageLabel," "),v(),he(e._displayedPageSizeOptions.length>1?3:-1),v(),he(e._displayedPageSizeOptions.length<=1?4:-1)}}function $H(t,n){if(t&1){let e=Ze();p(0,"button",19),A("click",function(){Ae(e);let r=W();return Re(r._buttonClicked(0,r._previousButtonsDisabled()))}),Ut(),p(1,"svg",8),F(2,"path",20),g()()}if(t&2){let e=W();k("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),te("aria-label",e._intl.firstPageLabel)}}function GH(t,n){if(t&1){let e=Ze();p(0,"button",21),A("click",function(){Ae(e);let r=W();return Re(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),Ut(),p(1,"svg",8),F(2,"path",22),g()()}if(t&2){let e=W();k("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),te("aria-label",e._intl.lastPageLabel)}}var WH=(()=>{class t{changes=new I;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,s=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${s} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),qH=50;var QH=new b("MAT_PAGINATOR_DEFAULT_OPTIONS"),Dd=(()=>{class t{_intl=d(WH);_changeDetectorRef=d(Ce);_formFieldAppearance;_pageSizeLabelId=d(We).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new An(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>an(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new Q;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(QH,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:s,showFirstLastButtons:a}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),s!=null&&(this.hidePageSize=s),a!=null&&(this.showFirstLastButtons=a)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:qH),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",an],length:[2,"length","length",an],pageSize:[2,"pageSize","pageSize",an],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",j],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",j],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",j]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(p(0,"div",1)(1,"div",2),me(2,HH,5,4,"div",3),p(3,"div",4)(4,"div",5),y(5),g(),me(6,$H,3,5,"button",6),p(7,"button",7),A("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),Ut(),p(8,"svg",8),F(9,"path",9),g()(),qo(),p(10,"button",10),A("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),Ut(),p(11,"svg",8),F(12,"path",11),g()(),me(13,GH,3,5,"button",12),g()()()),i&2&&(v(2),he(r.hidePageSize?-1:2),v(3),Oe(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),v(),he(r.showFirstLastButtons?6:-1),v(),k("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),te("aria-label",r._intl.previousPageLabel),v(3),k("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),te("aria-label",r._intl.nextPageLabel),v(3),he(r.showFirstLastButtons?13:-1))},dependencies:[Kt,oA,La,Cs,lA],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2,changeDetection:0})}return t})(),dA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[Nt,sA,cA,Dd]})}return t})();var KH=["mat-sort-header",""],ZH=["*",[["","matSortHeaderIcon",""]]],XH=["*","[matSortHeaderIcon]"];function JH(t,n){t&1&&(Ut(),Tt(0,"svg",3),gn(1,"path",4),At())}function e$(t,n){t&1&&(Tt(0,"div",2),U(1,1,null,JH,2,0),At())}var uA=new b("MAT_SORT_DEFAULT_OPTIONS"),xd=(()=>{class t{_defaultOptions;_initializedStream=new An(1);sortables=new Map;_stateChanges=new I;active;start="asc";get direction(){return this._direction}set direction(e){this._direction=e}_direction="";disableClear;disabled=!1;sortChange=new Q;initialized=this._initializedStream;constructor(e){this._defaultOptions=e}register(e){this.sortables.set(e.id,e)}deregister(e){this.sortables.delete(e.id)}sort(e){this.active!=e.id?(this.active=e.id,this.direction=e.start?e.start:this.start):this.direction=this.getNextSortDirection(e),this.sortChange.emit({active:this.active,direction:this.direction})}getNextSortDirection(e){if(!e)return"";let i=e?.disableClear??this.disableClear??!!this._defaultOptions?.disableClear,r=t$(e.start||this.start,i),o=r.indexOf(this.direction)+1;return o>=r.length&&(o=0),r[o]}ngOnInit(){this._initializedStream.next()}ngOnChanges(){this._stateChanges.next()}ngOnDestroy(){this._stateChanges.complete(),this._initializedStream.complete()}static \u0275fac=function(i){return new(i||t)(R(uA,8))};static \u0275dir=S({type:t,selectors:[["","matSort",""]],hostAttrs:[1,"mat-sort"],inputs:{active:[0,"matSortActive","active"],start:[0,"matSortStart","start"],direction:[0,"matSortDirection","direction"],disableClear:[2,"matSortDisableClear","disableClear",j],disabled:[2,"matSortDisabled","disabled",j]},outputs:{sortChange:"matSortChange"},exportAs:["matSort"],features:[Pe]})}return t})();function t$(t,n){let e=["asc","desc"];return t=="desc"&&e.reverse(),n||e.push(""),e}var fA=(()=>{class t{_sort=d(xd,{optional:!0});_columnDef=d(yo,{optional:!0});_changeDetectorRef=d(Ce);_focusMonitor=d(Mn);_elementRef=d(O);_ariaDescriber=d(dh,{optional:!0});_renderChanges;_animationsDisabled=Ve();_recentlyCleared=X(null);_sortButton;id;arrowPosition="after";start;disabled=!1;get sortActionDescription(){return this._sortActionDescription}set sortActionDescription(e){this._updateSortActionDescription(e)}_sortActionDescription="Sort";disableClear;constructor(){d(tt).load(Gn);let e=d(uA,{optional:!0});this._sort,e?.arrowPosition&&(this.arrowPosition=e?.arrowPosition)}ngOnInit(){!this.id&&this._columnDef&&(this.id=this._columnDef.name),this._sort.register(this),this._renderChanges=Et(this._sort._stateChanges,this._sort.sortChange).subscribe(()=>this._changeDetectorRef.markForCheck()),this._sortButton=this._elementRef.nativeElement.querySelector(".mat-sort-header-container"),this._updateSortActionDescription(this._sortActionDescription)}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(()=>{Promise.resolve().then(()=>this._recentlyCleared.set(null))})}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._sort.deregister(this),this._renderChanges?.unsubscribe(),this._sortButton&&this._ariaDescriber?.removeDescription(this._sortButton,this._sortActionDescription)}_toggleOnInteraction(){if(!this._isDisabled()){let e=this._isSorted(),i=this._sort.direction;this._sort.sort(this),this._recentlyCleared.set(e&&!this._isSorted()?i:null)}}_handleKeydown(e){(e.keyCode===32||e.keyCode===13)&&(e.preventDefault(),this._toggleOnInteraction())}_isSorted(){return this._sort.active==this.id&&(this._sort.direction==="asc"||this._sort.direction==="desc")}_isDisabled(){return this._sort.disabled||this.disabled}_getAriaSortAttribute(){return this._isSorted()?this._sort.direction=="asc"?"ascending":"descending":"none"}_renderArrow(){return!this._isDisabled()||this._isSorted()}_updateSortActionDescription(e){this._sortButton&&(this._ariaDescriber?.removeDescription(this._sortButton,this._sortActionDescription),this._ariaDescriber?.describe(this._sortButton,e)),this._sortActionDescription=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-sort-header",""]],hostAttrs:[1,"mat-sort-header"],hostVars:3,hostBindings:function(i,r){i&1&&A("click",function(){return r._toggleOnInteraction()})("keydown",function(s){return r._handleKeydown(s)})("mouseleave",function(){return r._recentlyCleared.set(null)}),i&2&&(te("aria-sort",r._getAriaSortAttribute()),N("mat-sort-header-disabled",r._isDisabled()))},inputs:{id:[0,"mat-sort-header","id"],arrowPosition:"arrowPosition",start:"start",disabled:[2,"disabled","disabled",j],sortActionDescription:"sortActionDescription",disableClear:[2,"disableClear","disableClear",j]},exportAs:["matSortHeader"],attrs:KH,ngContentSelectors:XH,decls:4,vars:17,consts:[[1,"mat-sort-header-container","mat-focus-indicator"],[1,"mat-sort-header-content"],[1,"mat-sort-header-arrow"],["viewBox","0 -960 960 960","focusable","false","aria-hidden","true"],["d","M440-240v-368L296-464l-56-56 240-240 240 240-56 56-144-144v368h-80Z"]],template:function(i,r){i&1&&(xe(ZH),Tt(0,"div",0)(1,"div",1),U(2),At(),me(3,e$,3,0,"div",2),At()),i&2&&(N("mat-sort-header-sorted",r._isSorted())("mat-sort-header-position-before",r.arrowPosition==="before")("mat-sort-header-descending",r._sort.direction==="desc")("mat-sort-header-ascending",r._sort.direction==="asc")("mat-sort-header-recently-cleared-ascending",r._recentlyCleared()==="asc")("mat-sort-header-recently-cleared-descending",r._recentlyCleared()==="desc")("mat-sort-header-animations-disabled",r._animationsDisabled),te("tabindex",r._isDisabled()?null:0)("role",r._isDisabled()?null:"button"),v(3),he(r._renderArrow()?3:-1))},styles:[`.mat-sort-header {
  cursor: pointer;
}

.mat-sort-header-disabled {
  cursor: default;
}

.mat-sort-header-container {
  display: flex;
  align-items: center;
  letter-spacing: normal;
  outline: 0;
}
[mat-sort-header].cdk-keyboard-focused .mat-sort-header-container, [mat-sort-header].cdk-program-focused .mat-sort-header-container {
  border-bottom: solid 1px currentColor;
}
.mat-sort-header-container::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-sort-header-content {
  display: flex;
  align-items: center;
}

.mat-sort-header-position-before {
  flex-direction: row-reverse;
}

@keyframes _mat-sort-header-recently-cleared-ascending {
  from {
    transform: translateY(0);
    opacity: 1;
  }
  to {
    transform: translateY(-25%);
    opacity: 0;
  }
}
@keyframes _mat-sort-header-recently-cleared-descending {
  from {
    transform: translateY(0) rotate(180deg);
    opacity: 1;
  }
  to {
    transform: translateY(25%) rotate(180deg);
    opacity: 0;
  }
}
.mat-sort-header-arrow {
  height: 12px;
  width: 12px;
  position: relative;
  transition: transform 225ms cubic-bezier(0.4, 0, 0.2, 1), opacity 225ms cubic-bezier(0.4, 0, 0.2, 1);
  opacity: 0;
  overflow: visible;
  color: var(--mat-sort-arrow-color, var(--mat-sys-on-surface));
}
.mat-sort-header.cdk-keyboard-focused .mat-sort-header-arrow, .mat-sort-header.cdk-program-focused .mat-sort-header-arrow, .mat-sort-header:hover .mat-sort-header-arrow {
  opacity: 0.54;
}
.mat-sort-header .mat-sort-header-sorted .mat-sort-header-arrow {
  opacity: 1;
}
.mat-sort-header-descending .mat-sort-header-arrow {
  transform: rotate(180deg);
}
.mat-sort-header-recently-cleared-ascending .mat-sort-header-arrow {
  transform: translateY(-25%);
}
.mat-sort-header-recently-cleared-ascending .mat-sort-header-arrow {
  transition: none;
  animation: _mat-sort-header-recently-cleared-ascending 225ms cubic-bezier(0.4, 0, 0.2, 1) forwards;
}
.mat-sort-header-recently-cleared-descending .mat-sort-header-arrow {
  transition: none;
  animation: _mat-sort-header-recently-cleared-descending 225ms cubic-bezier(0.4, 0, 0.2, 1) forwards;
}
.mat-sort-header-animations-disabled .mat-sort-header-arrow {
  transition-duration: 0ms;
  animation-duration: 0ms;
}
.mat-sort-header-arrow > svg, .mat-sort-header-arrow [matSortHeaderIcon] {
  width: 24px;
  height: 24px;
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  margin: -12px 0 0 -12px;
  transform: translateZ(0);
}
.mat-sort-header-arrow, [dir=rtl] .mat-sort-header-position-before .mat-sort-header-arrow {
  margin: 0 0 0 6px;
}
.mat-sort-header-position-before .mat-sort-header-arrow, [dir=rtl] .mat-sort-header-arrow {
  margin: 0 6px 0 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),mA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[_e]})}return t})();var i$=["input"],r$=["label"],o$=["*"],j0={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},s$=new b("mat-checkbox-default-options",{providedIn:"root",factory:()=>j0}),Zt=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(Zt||{}),z0=class{source;checked},U0=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Ce);_ngZone=d(z);_animationsDisabled=Ve();_options=d(s$,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new z0;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new Q;indeterminateChange=new Q;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=Zt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(tt).load(Gn);let e=d(new _n("tabindex"),{optional:!0});this._options=this._options||j0,this.color=this._options.color||j0.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(We).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(Zt.Indeterminate):this._transitionCheckState(this.checked?Zt.Checked:Zt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=X(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?Zt.Checked:Zt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case Zt.Init:if(i===Zt.Checked)return this._animationClasses.uncheckedToChecked;if(i==Zt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case Zt.Unchecked:return i===Zt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case Zt.Checked:return i===Zt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case Zt.Indeterminate:return i===Zt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&He(i$,5)(r$,5),i&2){let o;H(o=$())&&(r._inputElement=o.first),H(o=$())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(qt("id",r.id),te("tabindex",null)("aria-label",null)("aria-labelledby",null),on(r.color?"mat-"+r.color:"mat-accent"),N("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",j],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",j],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:an(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",j],checked:[2,"checked","checked",j],disabled:[2,"disabled","disabled",j],indeterminate:[2,"indeterminate","indeterminate",j]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[ge([{provide:ws,useExisting:kt(()=>t),multi:!0},{provide:Er,useExisting:t,multi:!0}]),Pe],ngContentSelectors:o$,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(xe(),p(0,"div",3),A("click",function(s){return r._preventBubblingFromLabel(s)}),p(1,"div",4,0)(3,"div",5),A("click",function(){return r._onTouchTargetClick()}),g(),p(4,"input",6,1),A("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),g(),F(6,"div",7),p(7,"div",8),Ut(),p(8,"svg",9),F(9,"path",10),g(),qo(),F(10,"div",11),g(),F(11,"div",12),g(),p(12,"label",13,2),U(14),g()()),i&2){let o=gt(2);k("labelPosition",r.labelPosition),v(4),N("mdc-checkbox--selected",r.checked),k("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),te("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),v(7),k("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),v(),k("for",r.inputId)}},dependencies:[Pa,ph],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})(),hA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=B({type:t});static \u0275inj=P({imports:[U0,_e]})}return t})();function l$(t,n){t&1&&(p(0,"mat-error"),y(1,"Name is required"),g())}function c$(t,n){t&1&&(p(0,"mat-error"),y(1,"Valid price is required"),g())}function d$(t,n){t&1&&(p(0,"mat-error"),y(1,"Valid price is required"),g())}function u$(t,n){t&1&&(p(0,"mat-error"),y(1,"Description is required"),g())}var lp=class t{constructor(n,e,i){this.fb=n;this.dialogRef=e;this.data=i;this.isEdit=!!i,this.productForm=this.fb.group({id:[i?.id],name:[i?.name||"",le.required],description:[i?.description||"",le.required],regularPrice:[i?.regularPrice||0,[le.required,le.min(0)]],salePrice:[i?.salePrice||0,[le.required,le.min(0)]],isActive:[i?i.isActive:!0],category:[i?.category||""],imageUrl:[i?.imageUrl||""]})}productForm;isEdit=!1;ngOnInit(){}onSubmit(){this.productForm.valid&&this.dialogRef.close(this.productForm.value)}static \u0275fac=function(e){return new(e||t)(R(Qi),R(Is),R(B0))};static \u0275cmp=T({type:t,selectors:[["app-product-dialog"]],decls:40,vars:7,consts:[["mat-dialog-title",""],["id","productForm",1,"product-form",3,"ngSubmit","formGroup"],["appearance","outline",1,"form-field-full"],["matInput","","formControlName","name","placeholder","E.g., Wireless Mouse"],[4,"ngIf"],["matInput","","formControlName","category","placeholder","E.g., Electronics"],[1,"price-row"],["appearance","outline",1,"form-field-half"],["matTextPrefix",""],["matInput","","type","number","formControlName","regularPrice","placeholder","0.00","min","0","step","0.01"],["matInput","","type","number","formControlName","salePrice","placeholder","0.00","min","0","step","0.01"],["formControlName","isActive","color","primary"],["matInput","","formControlName","description","rows","3","placeholder","Enter product details"],["align","end"],["mat-button","","type","button","mat-dialog-close",""],["mat-flat-button","","type","submit","form","productForm",1,"primary-button"]],template:function(e,i){if(e&1&&(p(0,"h2",0),y(1),g(),p(2,"mat-dialog-content")(3,"form",1),A("ngSubmit",function(){return i.onSubmit()}),p(4,"mat-form-field",2)(5,"mat-label"),y(6,"Product Name"),g(),F(7,"input",3),se(8,l$,2,0,"mat-error",4),g(),p(9,"mat-form-field",2)(10,"mat-label"),y(11,"Category"),g(),F(12,"input",5),g(),p(13,"div",6)(14,"mat-form-field",7)(15,"mat-label"),y(16,"Regular Price"),g(),p(17,"span",8),y(18,"$\xA0"),g(),F(19,"input",9),se(20,c$,2,0,"mat-error",4),g(),p(21,"mat-form-field",7)(22,"mat-label"),y(23,"Sale Price"),g(),p(24,"span",8),y(25,"$\xA0"),g(),F(26,"input",10),se(27,d$,2,0,"mat-error",4),g()(),p(28,"mat-checkbox",11),y(29,"Active"),g(),p(30,"mat-form-field",2)(31,"mat-label"),y(32,"Description"),g(),F(33,"textarea",12),se(34,u$,2,0,"mat-error",4),g()()(),p(35,"mat-dialog-actions",13)(36,"button",14),y(37,"Cancel"),g(),p(38,"button",15),y(39),g()()),e&2){let r,o,s,a;v(),Pt(i.isEdit?"Edit Product":"Add New Product"),v(2),k("formGroup",i.productForm),v(5),k("ngIf",(r=i.productForm.get("name"))==null?null:r.invalid),v(12),k("ngIf",(o=i.productForm.get("regularPrice"))==null?null:o.invalid),v(7),k("ngIf",(s=i.productForm.get("salePrice"))==null?null:s.invalid),v(7),k("ngIf",(a=i.productForm.get("description"))==null?null:a.invalid),v(5),Oe(" ",i.isEdit?"Update":"Create"," ")}},dependencies:[Rt,ui,Yi,qi,Wn,n0,Gi,Wi,i0,dn,_i,ap,XT,eA,nA,tA,Bt,Kt,un,Sr,s0,Qn,qn,Nt,wn,hA,U0],styles:[".product-form[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px;margin-top:16px;min-width:350px}.form-field-full[_ngcontent-%COMP%]{width:100%}.price-row[_ngcontent-%COMP%]{display:flex;gap:16px}.form-field-half[_ngcontent-%COMP%]{flex:1}"]})};var f$=()=>[5,10,25,100];function m$(t,n){t&1&&(p(0,"th",22),y(1," Name "),g())}function h$(t,n){if(t&1&&(p(0,"td",23),y(1),g()),t&2){let e=n.$implicit;v(),Oe(" ",e.name," ")}}function p$(t,n){t&1&&(p(0,"th",22),y(1," Category "),g())}function g$(t,n){if(t&1&&(p(0,"td",23),y(1),g()),t&2){let e=n.$implicit;v(),Oe(" ",e.category||"N/A"," ")}}function _$(t,n){t&1&&(p(0,"th",22),y(1," Regular Price "),g())}function v$(t,n){if(t&1&&(p(0,"td",23),y(1),Ff(2,"number"),g()),t&2){let e=n.$implicit;v(),Oe(" $",Pf(2,1,e.regularPrice,"1.2-2")," ")}}function y$(t,n){t&1&&(p(0,"th",22),y(1," Sale Price "),g())}function b$(t,n){if(t&1&&(p(0,"td",23),y(1),Ff(2,"number"),g()),t&2){let e=n.$implicit;v(),Oe(" $",Pf(2,1,e.salePrice,"1.2-2")," ")}}function w$(t,n){t&1&&(p(0,"th",22),y(1," Status "),g())}function C$(t,n){if(t&1&&(p(0,"td",23)(1,"span",24),y(2),g()()),t&2){let e=n.$implicit;v(),N("active",e.isActive)("inactive",!e.isActive),v(),Oe(" ",e.isActive?"Active":"Inactive"," ")}}function D$(t,n){t&1&&(p(0,"th",25),y(1," Actions "),g())}function x$(t,n){if(t&1){let e=Ze();p(0,"td",23)(1,"button",26),A("click",function(){let r=Ae(e).$implicit,o=W();return Re(o.openDialog(r))}),p(2,"mat-icon"),y(3,"edit"),g()(),p(4,"button",27),A("click",function(){let r=Ae(e).$implicit,o=W();return Re(o.deleteProduct(r.id))}),p(5,"mat-icon"),y(6,"delete"),g()()()}}function E$(t,n){t&1&&F(0,"tr",28)}function S$(t,n){t&1&&F(0,"tr",29)}function I$(t,n){if(t&1&&(p(0,"tr",30)(1,"td",31),y(2),g()()),t&2){W();let e=gt(12);v(2),Oe(' No products found matching the filter "',e.value,'" ')}}var cp=class t{constructor(n,e,i,r){this.productService=n;this.dialog=e;this.snackBar=i;this.cdr=r}dataSource=new rp([]);displayedColumns=["name","category","regularPrice","salePrice","isActive","actions"];paginator;sort;ngOnInit(){this.loadProducts()}loadProducts(){this.productService.getProducts().subscribe(n=>{this.dataSource.data=n,this.dataSource.paginator=this.paginator,this.dataSource.sort=this.sort,this.cdr.detectChanges()})}applyFilter(n){let e=n.target.value;this.dataSource.filter=e.trim().toLowerCase(),this.dataSource.paginator&&this.dataSource.paginator.firstPage()}openDialog(n){this.dialog.open(lp,{width:"90%",maxWidth:"400px",data:n||null}).afterClosed().subscribe(i=>{if(i)if(i.id)this.productService.updateProduct(i.id,i).subscribe({next:()=>{this.snackBar.open("Product updated successfully","Close",{duration:3e3}),this.loadProducts()},error:()=>this.snackBar.open("Error updating product","Close",{duration:3e3})});else{let r=w({},i);delete r.id,this.productService.addProduct(r).subscribe({next:()=>{this.snackBar.open("Product added successfully","Close",{duration:3e3}),this.loadProducts()},error:()=>this.snackBar.open("Error adding product","Close",{duration:3e3})})}})}deleteProduct(n){confirm("Are you sure you want to delete this product?")&&this.productService.deleteProduct(n).subscribe({next:()=>{this.snackBar.open("Product deleted","Close",{duration:3e3}),this.loadProducts()},error:()=>{this.snackBar.open("Error deleting product","Close",{duration:3e3})}})}static \u0275fac=function(e){return new(e||t)(R(el),R(Cd),R(wi),R(Ce))};static \u0275cmp=T({type:t,selectors:[["app-product-manager"]],viewQuery:function(e,i){if(e&1&&He(Dd,5)(xd,5),e&2){let r;H(r=$())&&(i.paginator=r.first),H(r=$())&&(i.sort=r.first)}},decls:39,vars:5,consts:[["input",""],[1,"header-container"],["mat-flat-button","",1,"primary-button",3,"click"],[1,"table-toolbar",2,"margin-bottom","16px"],["appearance","outline",2,"width","100%","max-width","400px"],["matInput","","placeholder","Ex. Wireless Mouse",3,"keyup"],["matSuffix",""],[1,"table-responsive",2,"overflow-x","auto"],["mat-table","","matSort","",1,"mat-elevation-z8","product-table",3,"dataSource"],["matColumnDef","name"],["mat-header-cell","","mat-sort-header","",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["matColumnDef","category"],["matColumnDef","regularPrice"],["matColumnDef","salePrice"],["matColumnDef","isActive"],["matColumnDef","actions"],["mat-header-cell","",4,"matHeaderCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["class","mat-row",4,"matNoDataRow"],["aria-label","Select page of products",1,"mat-elevation-z8",2,"margin-top","2px","border-radius","0 0 8px 8px",3,"pageSizeOptions"],["mat-header-cell","","mat-sort-header",""],["mat-cell",""],[1,"status-chip"],["mat-header-cell",""],["mat-icon-button","","color","primary",3,"click"],["mat-icon-button","","color","warn",3,"click"],["mat-header-row",""],["mat-row",""],[1,"mat-row"],["colspan","6",1,"mat-cell",2,"padding","24px","text-align","center"]],template:function(e,i){e&1&&(p(0,"div",1)(1,"h2"),y(2,"Product Inventory"),g(),p(3,"button",2),A("click",function(){return i.openDialog()}),p(4,"mat-icon"),y(5,"add"),g(),y(6," Add Product "),g()(),p(7,"div",3)(8,"mat-form-field",4)(9,"mat-label"),y(10,"Search Products"),g(),p(11,"input",5,0),A("keyup",function(o){return i.applyFilter(o)}),g(),p(13,"mat-icon",6),y(14,"search"),g()()(),p(15,"div",7)(16,"table",8),$t(17,9),se(18,m$,2,0,"th",10)(19,h$,2,1,"td",11),Gt(),$t(20,12),se(21,p$,2,0,"th",10)(22,g$,2,1,"td",11),Gt(),$t(23,13),se(24,_$,2,0,"th",10)(25,v$,3,4,"td",11),Gt(),$t(26,14),se(27,y$,2,0,"th",10)(28,b$,3,4,"td",11),Gt(),$t(29,15),se(30,w$,2,0,"th",10)(31,C$,3,5,"td",11),Gt(),$t(32,16),se(33,D$,2,0,"th",17)(34,x$,7,0,"td",11),Gt(),se(35,E$,1,0,"tr",18)(36,S$,1,0,"tr",19)(37,I$,3,1,"tr",20),g()(),F(38,"mat-paginator",21)),e&2&&(v(16),k("dataSource",i.dataSource),v(19),k("matHeaderRowDef",i.displayedColumns),v(),k("matRowDefColumns",i.displayedColumns),v(2),k("pageSizeOptions",pr(4,f$)))},dependencies:[Rt,GT,NT,PT,jT,LT,FT,zT,BT,VT,UT,HT,$T,Nt,wn,Cs,Xa,Za,ap,dA,Dd,mA,xd,fA,Bt,Kt,un,a0,Qn,qn,Gv],styles:[".header-container[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}.header-container[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin:0;color:var(--primary-color);font-weight:600}.product-table[_ngcontent-%COMP%]{width:100%;border-radius:var(--border-radius);overflow:hidden}.status-chip[_ngcontent-%COMP%]{padding:4px 12px;border-radius:16px;font-size:12px;font-weight:500;display:inline-block}.status-chip.active[_ngcontent-%COMP%]{background-color:#e8f5e9;color:#2e7d32}.status-chip.inactive[_ngcontent-%COMP%]{background-color:#ffebee;color:#c62828}"]})};var pA=[{path:"",redirectTo:"/register",pathMatch:"full"},{path:"login",component:jh},{path:"register",component:zh},{path:"forgot-password",component:Uh},{path:"dashboard",component:Qh,canActivate:[Yk],children:[{path:"",component:Yh},{path:"profile",component:Zh},{path:"products",component:cp}]}];var gA=(t,n)=>{let i=d(bn).getToken();if(i){let r=t.clone({setHeaders:{Authorization:`Bearer ${i}`}});return n(r)}return n(t)};var H0=class{isErrorState(n,e){let i=e&&e.submitted;return!!(n&&n.invalid&&i)}},_A={providers:[zg(),Uy(pA),dk(),ly(cy([gA])),{provide:ps,useClass:H0}]};var dp=class t{title=X("Frontend");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&F(0,"router-outlet")},dependencies:[cs],encapsulation:2})};ty(dp,_A).catch(t=>console.error(t));
