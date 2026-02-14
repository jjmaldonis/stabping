/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

'use strict';

const {h, render, Component} = window.preact;

const SENTINEL_ERROR = -2100000000;
const SENTINEL_NODATA = -2000000000;
const TARGET_KINDS = [
    {
        name: 'tcpping',
        prettyName: 'TCP Ping',
        addrsPrompt: 'Addresses (host:port) to ping',
        valFormatter: function(val) {
            return (val / 1000).toFixed() + ' ms';
        }
    }
    /*
    {
        name: 'httpdownload',
        pretty_name: 'HTTP Download',
        valFormatter: function(val) {
            return 'NOT YET IMPLEMENTED';
        }
    }
    */
];

/*
 * A self-reconnecting WebSocket that tries to re-establish a connection if it
 * becomes disconnected for whatever reason.
 */
class SPSocket {
    constructor(port, cb, interval) {
        if (!interval) {
            interval = 20000;
        }

        this.addr = 'ws://' + window.location.hostname + ':' + port;
        this.socket = this.newSocket(cb);

        setInterval(function() {
            if (this.socket.readyState > 1) {
                console.log('Reconnecting WebSocket...');
                this.socket = this.newSocket(cb);
            }
        }.bind(this), interval);
    }

    /*
     * Creates a new WebSocket connection to the previously specified address.
     */
    newSocket(cb) {
        var socket = new WebSocket(this.addr);
        socket.binaryType = 'arraybuffer';
        socket.onmessage = cb;
        return socket;
    }
}

/*
 * Performs an AJAX (XMLHttpRequest) request where
 *     - method is the HTTP verb to use (e.g. 'POST')
 *     - dest is the destination endpoint path (e.g. '/api/endpoint')
 *     - type concerns how the response should be handled (e.g. 'json')
 *     - success is callback function that takes a response body
 *     - error is a callback funtion that takes a full error response object
 *     - data is data to send to the server (as in 'POST')
 */
function ajax(method, dest, type, success, error, data) {
    var req = new XMLHttpRequest();
    req.responseType = type;
    req.open(method, dest, true);
    req.onreadystatechange = function() {
        if (req.readyState == 4) {
            if (req.status == 200) {
                if (success) {
                    success(req.response);
                }
            } else {
                if (error) {
                    error(req);
                }
            }
        }
    }
    req.send(data);
}

/*
 * Gets the current time in seconds since epoch.
 */
function currentTime() {
    return Math.floor(new Date() / 1000);
}

// the time in seconds since epoch when the page was loaded
var timeLoaded = currentTime();

/*
 * Converts a specified number of "hours back" as in '2 hours back' into a time
 * in seconds from epoch.
 *
 * Two sentinel values:
 *     -2 -> time since ever (aka. epoch 0)
 *     -1 -> time page was loaded
 */
function hoursBack(hours) {
    if (hours == -2) {
        // All
        return 0;
    } else if (hours == -1) {
        // Since Load
        return timeLoaded;
    } else {
        // Some number of hours
        return currentTime() - (hours * 3600);
    }
}

/*
 * A Dygraph axis formatter for dates represented as seconds since epoch.
 */
function dateAxisFormatter(epochSeconds, gran, opts) {
    return Dygraph.dateAxisLabelFormatter(new Date(epochSeconds * 1000), gran, opts);
}

/*
 * A Dygraph legend text formatter for dates represented in seconds since epoch.
 */
function dateFormatter(epochSeconds) {
    return Dygraph.dateString_(epochSeconds * 1000);
}

// the default (automatically-Dygraph-recalculating value range)
var autoValueRange = [0, null];

/*
 * A Component wrapping the Dygraph div and canvas.
 */
class Graph extends Component {
    constructor() {
        super();
        this.graph = null;
    }

    componentDidMount() {
        // initialize the Dygraph when this component mounts
        var gvFormatter = function(val, opts, seriesName) {
            if (seriesName == 'Time') {
                return dateFormatter(val);
            } else {
                return this.props.valFormatter(val);
            }
        }.bind(this);

        this.graph = new Dygraph(
            this.base,  // the root div of this Component
            [[0]],
            {
                animatedZooms: true,
                valueFormatter: gvFormatter,
                valueRange: autoValueRange,
                axes: {
                    x: {
                        axisLabelFormatter: dateAxisFormatter
                    },
                    y: {
                        axisLabelFormatter: this.props.valFormatter
                    }
                },
                isZoomedIgnoreProgrammaticZoom: true,
                zoomCallback: function (lowerDate, upperDate, yRanges) {
                    // update the graph with new data when the user unzooms
                    this.update();
                }.bind(this)
            }
        );
    }

    /*
     * Basically this Component's render() method for updating the Dygraph, as
     * the actual render method is skipped to prevent DOM diffing from
     * trampling Dygraph's canvas.
     */
    update() {
        if (!this.graph || !this.props.data) return;

        // object containing all the graph options we want to update
        var g = {};

        /*
         * only update data points, change axes, or alter range if user has not
         * zoomed in on graph to prevent annoying surprises
         */
        if (!this.graph.isZoomed()) {
            g.isZoomedIgnoreProgrammaticZoom = true;
            g.labels = ['Time'].concat(this.props.options.addrs);

            var h = hoursBack(this.props.preset);
            g.dateWindow = h == 0 ? null : [h, this.props.data.slice(-1)[0][0]];

            g.file = this.props.data;
        }

        if (this.graph.getOption('rollPeriod') != this.props.rollPeriod) {
            g.rollPeriod = this.props.rollPeriod;
        }

        // if there are any changes we need to make, tell Dygraph to make them
        if (Object.keys(g).length > 0) {
            this.graph.updateOptions(g);
        }
    }

    shouldComponentUpdate() {
        // prevent DOM diffing from trampling Dygraph's canvas
        return false;
    }

    render() {
        // just a div for Dygraph to manage
        return h('div', {
            className: 'graph'
        });
    }
}

/*
 * A Component encapsulating all the options-adjusting controls that maintains
 * a separate options state until the user explicitly clicks 'Save'.
 */
class Options extends Component {
    componentWillMount() {
        /*
         * retrieve the current state of this target's options, and make a
         * separate copy of it that will manage this component's UI elements
         */
        this.state = JSON.parse(JSON.stringify(this.props.options))
        this.state.addrInput = '';
    }

    /*
     * Retrieves the state of the user-updated options in this Component.
     */
    getOptions() {
        delete this.state.addrInput;
        return this.state;
    }

    render() {
        return h('div', {className: 'options-container'}, [
            h('h3', null, this.props.kind.prettyName + ' Options'),

            // UI element for adjusting the interval
            h('div', null, [
                'Collect data every',
                h('input', {
                    type: 'number',
                    value: this.state.interval / 1000,
                    onInput: (evt) => this.setState({interval: evt.target.value * 1000}),
                    title: 'seconds'
                }),
                's'
            ]),

            // UI element for adjusting avg_across
            h('div', null, [
                'Avg across',
                h('input', {
                    type: 'number',
                    value: this.state.avg_across,
                    onInput: (evt) => this.setState({avg_across: parseInt(evt.target.value, 10)})
                }),
                'values'
            ]),

            // UI elements for editing addrs
            h('div', null, [
                this.props.kind.addrsPrompt,
                h('ul', null, [
                    this.state.addrs.map(function(val, i, arr) {
                        return h('li', {className: 'addr-item'}, [
                            h('button', {
                                onClick: () => {
                                    arr.splice(i, 1);
                                    this.setState({addrs: arr});
                                }
                            }, '-'),
                            val
                        ]);
                    }.bind(this))
                ]),
                h('div', {className: 'addr-input'}, [
                    h('input', {
                        type: 'text',
                        value: this.state.addrInput,
                        onInput: (evt) => this.setState({addrInput: evt.target.value})
                    }),
                    h('button', {
                        onClick: () => {
                            var addrs = this.state.addrs;
                            addrs.push(this.state.addrInput);
                            this.setState({
                                addrInput: '',
                                addrs: addrs
                            });
                        }
                    }, 'Add')
                ])
            ])
        ])
    }
}

/*
 * Component in charge of everything related to a target, including retrieving
 * persistent data, managing the graph, adding live data to the graph, and
 * handling options.
 */
class Target extends Component {
    constructor(props) {
        super(props);
        this.state = {
            // the target's current options
            options: {},

            // the lower bound on how much persisted data we have in-browser
            leftLimit: currentTime(),

            /*
             * the user-selected base time interval in terms of "hours back" of
             * how much (persisted) data to retrieve/display
             */
            preset: 1,

            // the user-inputted number of points to do rolling average over
            rollPeriod: 1,

            /*
             * whether or not the user is currently editing this target's
             * options (and thus we're displaying the options editing inteface)
             */
            optionsMode: false
        };
    }

    componentDidMount() {
        // fetch information about this target from the server on load
        ajax('GET', '/api/target/' + this.props.kind.name, 'json', function(res) {
            console.log('Fetched option for: ' + this.props.kind.name);
            this.setState({
                options: res
            });

            /*
             * fetch the necessary persistent data (to satisfy display of the
             * default preset
             */
            setTimeout(function() {
                this.persistentDataRetrieve(this.state.preset);
            }.bind(this), 300);
        }.bind(this));
    }

    /*
     * Retrieves persistent data for this target from the server for the given
     * number of "hours back".
     */
    persistentDataRetrieve(hoursPreset) {
        if (hoursPreset == 0) {
            return;
        }

        // we will be missing the data for times in [leftTarget, leftLimit]
        var leftTarget = hoursBack(hoursPreset);
        var leftLimit = this.state.leftLimit;

        var elementLength = this.state.options.addrs.length + 1;
        var nonce = this.state.options.nonce;

        // only hit the server for the data if we don't already have it in-browser
        if (leftTarget < leftLimit) {
            ajax('POST', '/api/target/' + this.props.kind.name, 'arraybuffer', function(res) {
                if (nonce == this.state.options.nonce) {
                    // read the response from the server as a Int32 Typed Array
                    var raw = new Int32Array(res);

                    // pre-allocate a large buffer array that will be assimilated into this.data
                    var newData = new Array(Math.ceil(raw.length / elementLength));
                    let k = 0;

                    /*
                     * Loop through the block of server data in time-delimited
                     * segments, creating a new [time, datapoint1, datapoint2, ...]
                     * array for each segment and appending it to newData.
                     */
                    for (let j = 0; j < raw.length; j += elementLength) {
                        let arr = new Array(elementLength);
                        for (let i = 0; i < arr.length; i++) {
                            let n = raw[j + i];
                            arr[i] = n >= 0 ? n : null;
                        }
                        newData[k++] = arr;
                    }

                    /*
                     * our pre-allocation may have been one-off, if so, drop
                     * the (unused) last element
                     */
                    if (newData[newData.length - 1] == undefined) {
                        newData.pop();
                    }

                    // assimilate these new data with the existing data in this.data
                    if (this.data) {
                        this.data = newData.concat(this.data);
                    } else {
                        this.data = newData;
                    }

                    // set a new leftLimit reflecting the data we just got
                    this.setState({
                        leftLimit: leftTarget
                    });
                } else {
                    console.log('Nonce changed since persistent data retrieve!');
                }
            }.bind(this), function(err) {
                console.log('Failed to retrieve persistent data for range ' + leftTarget + ' to ' + leftLimit);
            }.bind(this), JSON.stringify({
                nonce: this.state.options.nonce,
                lower: leftTarget,
                upper: leftLimit
            }));
        }
    }

    onPresetChange(evt) {
        // retrieve persistent data if necessary to fulfill new preset
        this.persistentDataRetrieve(evt.target.value);
        this.setState({preset: evt.target.value});
    }

    onSaveOptions() {
        if (this.state.optionsMode) {
            // retrieve user-edited new options from the Options Component
            var newOpts = this.optionsComponent.getOptions();
            // retrieve the current options from this.state
            var curOpts = this.state.options;

            // diff new options and current options and only hit server if different
            console.log('Checking options for differences...');

            // diff the independent fields
            var optsChanged = newOpts.interval != curOpts.interval ||
                              newOpts.avg_across != curOpts.avg_across ||
                              newOpts.pause != curOpts.pause;

            // diff the addrs
            var addrsChanged = newOpts.addrs.length != curOpts.addrs.length;
            for (let i = 0; !addrsChanged && i < newOpts.addrs.length; i++) {
                if (newOpts.addrs[i] != curOpts.addrs[i]) {
                    addrsChanged = true;
                }
            }

            /*
             * hit the server with an options update only if user changed
             * something
             */
            if (optsChanged || addrsChanged) {
                console.log('Saving options to server...');
                ajax('PUT', '/api/target/' + this.props.kind.name, 'text', function(res) {
                    console.log('Server accepted options update.');
                    var newNonce = parseInt(res, 10);
                    newOpts.nonce = newNonce;
                    var newState = {
                        options: newOpts,
                        optionsMode: false
                    };
                    if (addrsChanged) {
                        /*
                         * invalidate the graph and all in-browser data if
                         * addrs changed
                         */
                        this.data = null;
                        newState.leftLimit = currentTime();
                    }

                    // set the new options and retrieve data that may now be needed
                    this.setState(newState, function() {
                        this.persistentDataRetrieve(this.state.preset);
                    }.bind(this));
                }.bind(this), function(err) {
                    console.log('Failed to update options on server! ' + err);
                }.bind(this), JSON.stringify(newOpts))
            }

            delete this.optionsComponent;
        }
    }

    render() {
        let buttons, controls;
        if (this.state.optionsMode) {
            /*
             * UI elements for options editing, including the 'Save' and
             * 'Cancel' buttons and the Options Component itself
             */
            buttons = [
                h('button', {
                    onClick: () => this.setState({optionsMode: false})
                }, 'Cancel'),
                h('button', {
                    className: 'btn-primary',
                    onClick: this.onSaveOptions.bind(this)
                }, 'Save')
            ];
            controls = h(Options, {
                ref: (o) => {
                    this.optionsComponent = o;
                },
                kind: this.props.kind,
                options: this.state.options
            });
        } else {
            // button for switching to options editing mode
            buttons = h('button', {
                className: 'btn-icon',
                onClick: () => this.setState({optionsMode: true})
            }, '⚙');

            // graph and data control UI elements
            controls = [
                h('label', {className: 'select-label'}, 'Base Time Interval'),
                h('select', {
                    className: 'base-interval-select',
                    value: this.state.preset,
                    onChange: this.onPresetChange.bind(this)
                }, [
                    h('option', {value: -1}, 'Since Load'),
                    h('option', {value: 1/60}, '1 Minute'),
                    h('option', {value: 5/60}, '5 Minutes'),
                    h('option', {value: 0.25}, '15 Minutes'),
                    h('option', {value: 0.5}, '30 Minutes'),
                    h('option', {value: 1}, '1 Hour'),
                    h('option', {value: 3}, '3 Hours'),
                    h('option', {value: 6}, '6 Hours'),
                    h('option', {value: 12}, '12 Hours'),
                    h('option', {value: 24}, '1 Day'),
                    h('option', {value: 72}, '3 Days'),
                    h('option', {value: 168}, '1 Week'),
                    h('option', {value: 336}, '2 Weeks'),
                    h('option', {value: 744}, '1 Month'),
                    h('option', {value: -2}, 'All*')
                ]),
                h('span', null, [
                    'Roll avg over',
                    h('input', {
                        type: 'number',
                        value: this.state.rollPeriod,
                        onInput: (evt) => this.setState({rollPeriod: evt.target.value})
                    }),
                    'point(s)'
                ])
            ];
        }

        return h('div', {
            className: 'graph-container'
        }, [
            // stick the buttons with the graph title for aesthetics
            h('div', {className: 'target-head'}, [
                h('h2', null, this.props.kind.prettyName),
                h('div', {className: 'button-container'}, buttons)
            ]),

            // the actual Graph Component itself
            h(Graph, {
                ref: (g) => {
                    g.update();
                },
                kind: this.props.kind,
                valFormatter: this.props.valFormatter,
                data: this.data,
                options: this.state.options,
                preset: this.state.preset,
                rollPeriod: this.state.rollPeriod
            }),

            /*
             * the "controls", either graph and data controls, or the options-
             * editing UI
             */
            h('div', {className: 'graph-controls'}, controls)
        ]);
    }

    /*
     * Updates this target's in-browser data and the graph with live data
     * coming from the server
     */
    liveDataUpdate(nonce, inArr) {
        if (nonce != this.state.options.nonce) {
            console.log('Mismatched nonce! I have ' + this.state.options.nonce +
                        ' but the server gave me ' + nonce);
            console.log(arr);
        }

        // if this.data doesn't exist yet, make it a new array
        if (!this.data) {
            this.data = [];
        }

        /*
         * read in the actual data, converting it to Dygraph-friendly format,
         * and append it to this.data
         */
        var arr = new Array(inArr.length);
        for (let i = 0; i < arr.length; i++) {
            let n = inArr[i];
            arr[i] = n >= 0 ? n : null;
        }
        this.data.push(arr);

        /*
         * force update on DOM diffing of UI components (as we are managing
         * this.data separately from the automatically-diffed this.state to
         * avoid expensive re-allocations)
         */
        this.forceUpdate();
    }
}

/*
 * Root Component containing all the Target Components, and handling the
 * websockets connection.
 */
class App extends Component {
    constructor() {
        super();
        this.targets = new Array(TARGET_KINDS.length);
    }

    handleSocketMessage(message) {
        // on receiving a websockets message, read it as an Int32 Typed Array
        var buf = message.data;
        var raw = new Int32Array(buf);

        // separate the target kind and nonce from the actual data
        var kind_id = raw[0];
        var nonce = raw[1];
        var arr = raw.slice(2);

        // live-update the appropriate target
        this.targets[kind_id].liveDataUpdate(nonce, arr);
    }

    componentDidMount() {
        // connect websockets on load
        ajax('GET', '/api/config/ws_port', 'text', function(port_str) {
            new SPSocket(port_str, this.handleSocketMessage.bind(this));
        }.bind(this));
    }

    render() {
        // an array stroing all the Target Components
        var target_components = [];

        // initialize all a Target Component for each target kind
        for (let i = 0; i < TARGET_KINDS.length; i++) {
            let kind = TARGET_KINDS[i];
            target_components.push(h(Target, {
                ref: (t) => {
                    this.targets[i] = t;
                },
                kind: kind,
                valFormatter: kind.valFormatter
            }));
        }

        return h('div', null, target_components);
    }
}

// mount the root App Component into the DOM
render(h(App), document.body);
